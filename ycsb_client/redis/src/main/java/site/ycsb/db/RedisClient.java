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
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a> using Lettuce client.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private RedisCommands<String, String> redisCommands;
  private StatefulRedisConnection<String, String> connection;
  private RedisAdvancedClusterCommands<String, String> clusterCommands;
  private StatefulRedisClusterConnection<String, String> clusterConnection;
  private boolean isCluster = false;

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
      port = 6379; // Default Redis port
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      isCluster = true;
      RedisClusterClient clusterClient = RedisClusterClient.create("redis://" + host + ":" + port);
      clusterConnection = clusterClient.connect();
      clusterCommands = clusterConnection.sync();
    } else {
      isCluster = false;
      RedisURI redisURI = RedisURI.create(host, port);
      
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null) {
        redisURI.setTimeout(java.time.Duration.ofMillis(Integer.parseInt(redisTimeout)));
      }
      
      String password = props.getProperty(PASSWORD_PROPERTY);
      if (password != null) {
        redisURI.setPassword(password);
      }
      
      RedisClient client = RedisClient.create(redisURI);
      connection = client.connect();
      redisCommands = connection.sync();
    }
  }

  public void cleanup() throws DBException {
    try {
      if (isCluster) {
        if (clusterConnection != null) {
          clusterConnection.close();
        }
      } else {
        if (connection != null) {
          connection.close();
        }
      }
    } catch (Exception e) {
      throw new DBException("Closing connection failed.", e);
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

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      if (fields == null) {
        Map<String, String> hashMap;
        if (isCluster) {
          hashMap = clusterCommands.hgetall(key);
        } else {
          hashMap = redisCommands.hgetall(key);
        }
        StringByteIterator.putAllAsByteIterators(result, hashMap);
      } else {
        String[] fieldArray = fields.toArray(new String[fields.size()]);
        List<String> values;
        if (isCluster) {
          values = clusterCommands.hmget(key, fieldArray);
        } else {
          values = redisCommands.hmget(key, fieldArray);
        }

        Iterator<String> fieldIterator = fields.iterator();
        Iterator<String> valueIterator = values.iterator();

        while (fieldIterator.hasNext() && valueIterator.hasNext()) {
          String value = valueIterator.next();
          if (value != null) {
            result.put(fieldIterator.next(), new StringByteIterator(value));
          } else {
            fieldIterator.next(); // Skip this field
          }
        }
        assert !fieldIterator.hasNext() && !valueIterator.hasNext();
      }
      return result.isEmpty() ? Status.ERROR : Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> stringMap = StringByteIterator.getStringMap(values);
      if (isCluster) {
        clusterCommands.hmset(key, stringMap);
        clusterCommands.zadd(INDEX_KEY, hash(key), key);
      } else {
        redisCommands.hmset(key, stringMap);
        redisCommands.zadd(INDEX_KEY, hash(key), key);
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      Long delResult, zremResult;
      if (isCluster) {
        delResult = clusterCommands.del(key);
        zremResult = clusterCommands.zrem(INDEX_KEY, key);
      } else {
        delResult = redisCommands.del(key);
        zremResult = redisCommands.zrem(INDEX_KEY, key);
      }
      return (delResult == 0 && zremResult == 0) ? Status.ERROR : Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      Map<String, String> stringMap = StringByteIterator.getStringMap(values);
      if (isCluster) {
        clusterCommands.hmset(key, stringMap);
      } else {
        redisCommands.hmset(key, stringMap);
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      Set<String> keys;
      if (isCluster) {
        keys = clusterCommands.zrangebyscore(INDEX_KEY, hash(startkey),
            Double.POSITIVE_INFINITY, 0, recordcount);
      } else {
        keys = redisCommands.zrangebyscore(INDEX_KEY, hash(startkey),
            Double.POSITIVE_INFINITY, 0, recordcount);
      }

      HashMap<String, ByteIterator> values;
      for (String key : keys) {
        values = new HashMap<String, ByteIterator>();
        read(table, key, fields, values);
        result.add(values);
      }

      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

}
