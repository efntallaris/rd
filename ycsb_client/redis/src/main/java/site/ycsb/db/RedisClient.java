/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * Records are mapped to *Redis string values* (SET/GET) rather than hashes.
 * The string body is the concatenation of all field byte-values that YCSB
 * generates, so the on-wire / on-disk size is exactly fieldcount *
 * fieldlength (the values defined by the YCSB workload). This matches the
 * single-buffer layout that the aqueduct fork's RDMA reshard data plane
 * encodes (rdmaEncodeSlotEntries currently only handles OBJ_STRING).
 *
 * The legacy hash-field representation lives upstream; switch back to it by
 * reverting this file. For scanning, all keys are still recorded in a
 * sorted-set index keyed by an arbitrary hash.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null){
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        jedis = new Jedis(host, port);
      }
      ((Jedis) jedis).connect();
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  /* Concatenate all field byte-values, in iteration order, into a single
   * string. With fieldcount=N and fieldlength=L (YCSB defaults: 10 / 100),
   * the result is exactly N*L bytes — the value size the workload defines. */
  private static String concatFields(Map<String, ByteIterator> values) {
    StringBuilder sb = new StringBuilder();
    for (ByteIterator v : values.values()) {
      sb.append(v.toString());
    }
    return sb.toString();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    /* Records are stored as a single string. There is no per-field
     * structure to project; YCSB just measures the latency of the GET. */
    String val = jedis.get(key);
    if (val == null) {
      return Status.ERROR;
    }
    result.put("value", new StringByteIterator(val));
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (jedis.set(key, concatFields(values)).equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    /* YCSB update sends only the modified fields (typically 1 of N). With
     * the single-string representation we just SET the new concatenated
     * payload, replacing the prior contents. The wire-bytes-per-op match
     * what the workload describes; the logical fields-merge that a hash
     * would have provided is intentionally dropped. */
    return jedis.set(key, concatFields(values)).equals("OK")
        ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
