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
//import site.ycsb.StringByteIterator;
import java.io.Closeable;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.*;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

//import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.List;
 
// STRATOS LETTUCE
import io.lettuce.core.*;
import java.util.concurrent.TimeUnit ;
import java.time.Duration;
//STRATOS TIMESTAMPS
//import java.sql.Timestamp;
//import java.text.SimpleDateFormat;
//import java.util.Date;

//ANTONIS
import java.io.DataOutputStream;
import java.io.FileOutputStream;


/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;
  private io.lettuce.core.cluster.api.sync.RedisClusterCommands jedis2;
  private io.lettuce.core.cluster.RedisClusterClient redisClusterClient;

  // Log data to binary file
  private DataOutputStream dataLogger = null;
  private boolean isDataLogEnabled = false;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String WRITE_FILE_PROPERTY = "redis.logfile";

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
      GenericObjectPoolConfig config = new GenericObjectPoolConfig();
      jedis = new JedisCluster(jedisClusterNodes, 100000, 100000, 300, config);


      //LETUCE
      io.lettuce.core.RedisURI redisURI = new io.lettuce.core.RedisURI(host, port, Duration.ofSeconds(60));
      io.lettuce.core.resource.ClientResources clientResources = io.lettuce.core.resource
           .ClientResources
           .builder()
           .ioThreadPoolSize(60)
           .computationThreadPoolSize(60)
           .build();

      redisClusterClient = io.lettuce.core.cluster.RedisClusterClient.create(clientResources, redisURI);
      io.lettuce.core.cluster.ClusterTopologyRefreshOptions topologyRefreshOptions = io.lettuce.core.cluster
           .ClusterTopologyRefreshOptions
           .builder()
           .enableAllAdaptiveRefreshTriggers()
           .dynamicRefreshSources(true)
           .enablePeriodicRefresh()
           .refreshPeriod(Duration.ofSeconds(1))
           .build();

      io.lettuce.core.cluster.ClusterClientOptions clusterOptions = io.lettuce.core.cluster
           .ClusterClientOptions
           .builder()
           .maxRedirects(15)
           .topologyRefreshOptions(topologyRefreshOptions)
           .build();
      redisClusterClient.setOptions(clusterOptions);
      io.lettuce.core.cluster.api.StatefulRedisClusterConnection<String, String> connection = 
           redisClusterClient.connect();
      redisClusterClient.reloadPartitions();
      jedis2 = connection.sync();
      io.lettuce.core.cluster.models.partitions.Partitions clusterPartitions = connection.getPartitions();

      // prepare key value writer
      //
      String datalogFileName = props.getProperty(WRITE_FILE_PROPERTY);
      initDataLogger(datalogFileName);
      /*System.out.println(connection.getResources().ioThreadPoolSize());
      System.out.println(clusterPartitions.toString());*/
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
    closeDataLogger();

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
  public void printSlots(){

	    String redisHost = "192.168.20.1";
            int redisPort = 8000;
	    try (JedisCluster jedisCluster = new JedisCluster(new HostAndPort(redisHost, redisPort))) {
//		    List<Object> slots = jedisCluster.clusterSlots();
//
//		    // Process and print the slot information
//		    for (Object slotInfo : slots) {
//			List<Object> slotInfoList = (List<Object>) slotInfo;
//			System.out.println("Slot Range: " + slotInfoList.get(0) + " - " + slotInfoList.get(1));
//
//			List<Object> nodeInfo = (List<Object>) slotInfoList.get(2);
//			System.out.println("Responsible Node: " + new String((byte[]) nodeInfo.get(0)) + ":" + nodeInfo.get(1));
//		    } 


	    } catch (Exception e) {
		    e.printStackTrace();
	    }

  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    Object value = jedis2.get(key);
    // String value = jedis.get(key);
    if(value != null){
      return Status.OK;
    }
    if (isDataLogEnabled) {
      logData(key, "");
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    String valueAllColumns = "";
    for(Map.Entry<String, ByteIterator> entry : values.entrySet()){
      valueAllColumns += entry.getKey()+"_"+entry.getValue();
    }



    String resultSet = jedis2.set(key, valueAllColumns);
    if (resultSet.equals("OK")) {
      // Log data
      if (isDataLogEnabled) {
	logData(key, valueAllColumns);
      }
      return Status.OK;
    } else {
      // System.out.println("Error " + resultSet);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    String bigString = "";
    for(Map.Entry<String, ByteIterator> entry : values.entrySet()){
      bigString += entry.getKey()+"_"+entry.getValue();
    }
    if (jedis.set(key, bigString).equals("OK")){
      return Status.OK;
    }
    return Status.ERROR;
//    return jedis.hmset(key, StringByteIterator.getStringMap(values))
//        .equals("OK") ? Status.OK : Status.ERROR;
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

  public void initDataLogger(String dataFileName) {
    if (dataFileName == null) {
      isDataLogEnabled = false;
      return;
    }
    try {
      dataFileName += Thread.currentThread().getId();
      dataLogger = new DataOutputStream(new FileOutputStream(dataFileName, false));
      isDataLogEnabled = true;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void logData(String key, String value) {
    try {
      dataLogger.writeInt(key.length());
      dataLogger.writeInt(value.length());
      dataLogger.writeBytes(key);
      dataLogger.writeBytes(value);
    } catch (IOException e) {
      System.err.println("Error loging data to file");
      e.printStackTrace();
    }
  }

  public void closeDataLogger() {
    try {
      if (dataLogger != null) {
        dataLogger.flush();
        dataLogger.close();
        dataLogger = null;
        isDataLogEnabled = false;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
