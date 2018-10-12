package io.snappydata.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

/**
 * JDBC Connection Pool for the Client Pool Driver.
 */
public final class JDBCConnectionTable {

  /**
   * Used for the max number of connection a pool can have.
   */
  private static final int NUMBER_OF_CONNECTION =
      Math.max(256, Runtime.getRuntime().availableProcessors() * 8);

  private static JDBCConnectionTable jdbcConnectionTable;

  /**
   * Different pool properties with key and default values.
   */
  static enum PoolProps {

    TEST_ON_BORROW("pool.testOnBorrow", "true"),
    TEST_ON_RETURN("pool.testOnReturn", "true"),
    TEST_ON_CREATE("pool.testOnCreate", "false"),
    TEST_WHILE_IDLE("pool.testWhileIdle", "false"),

    TIME_BETWEEN_EVICTION_RUNS_MILLIS("pool.timeBetweenEvictionRunsMillis",
        String.valueOf(CONNECTION_TIMEOUT * 5)),
    MIN_EVICTABLE_IDLE_TIME_MILLIS("pool.minEvictableIdleTimeMillis",
        String.valueOf(CONNECTION_TIMEOUT)),

    MAX_WAIT_IN_MILLIS("pool.maxWaitInMillis", String.valueOf(CONNECTION_TIMEOUT)),

    MAX_IDLE_PER_KEY("pool.maxIdlePerKey", String.valueOf(NUMBER_OF_CONNECTION)),
    MAX_TOTAL_PER_KEY("pool.maxTotalPerKey", String.valueOf(NUMBER_OF_CONNECTION)),
    MIN_IDLE_PER_KEY("pool.minIdlePerKey", "10"),

    MAX_TOTAL("pool.maxTotal", String.valueOf(NUMBER_OF_CONNECTION)),

    LIFO("pool.lifo", "true"),
    BLOCK_WHEN_EXHAUSTED("pool.blockWhenExhausted", "true"),
    FAIRNESS("pool.fairness", "false"),
    URL("pool.url", ""); // Compulsory field user must provide

    final String key;
    final String defValue;

    PoolProps(String propKey, String defValue) {
      this.key = propKey;
      this.defValue = defValue;
    }
  }

  private static final int CONNECTION_TIMEOUT = 60000;

  /**
   *
   */
  private final GenericKeyedObjectPool<Properties, Connection> connectionPool;

  /**
   *
   */
  private JDBCConnectionTable() throws Exception {

    KeyedPooledObjectFactory<Properties, Connection> factory =
        new KeyedPooledObjectFactory<Properties, Connection>() {

      @Override
      public PooledObject<Connection> makeObject(Properties properties) throws Exception {
        String url = properties.getProperty(JDBCConnectionTable.PoolProps.URL.key);
        Properties connProps = new Properties(properties);
        for (Object key : connProps.keySet()) {
          if (key.toString().startsWith("pool.")) {
            connProps.remove(key);
          }
        }
        Connection connection = DriverManager.getConnection(url, connProps);
        return new DefaultPooledObject<>(connection);
      }

      @Override
      public void destroyObject(Properties properties, PooledObject<Connection> p) throws Exception {
        if (!p.getObject().isClosed()) {
          p.getObject().close();
        }
      }

      @Override
      public boolean validateObject(Properties properties, PooledObject<Connection> p) {
        return true;
      }

      @Override
      public void activateObject(Properties properties,
          PooledObject<Connection> p) throws Exception {
        // TODO: Handle connection reset operation to default, while connection accessed from the pool.
        // p.getObject().setTransactionIsolation(Connection.TRANSACTION_NONE);
        // p.getObject().setAutoCommit(true);
      }

      @Override
      public void passivateObject(Properties properties,
          PooledObject<Connection> p) throws Exception {
        // TODO: Handle connection reset operation, while connection returning to the pool.
        // p.getObject().setTransactionIsolation(Connection.TRANSACTION_NONE);
        // p.getObject().setAutoCommit(true);
      }
    };

    connectionPool = new GenericKeyedObjectPool<Properties, Connection>(factory);
    connectionPool.setConfig(setAndGetPoolConfigProperties(new Properties()));
  }

  public static JDBCConnectionTable create() throws Exception {
    if (jdbcConnectionTable == null) {
      synchronized (JDBCConnectionTable.class) {
        if (jdbcConnectionTable == null) {
          jdbcConnectionTable = new JDBCConnectionTable();
        }
      }
    }
    return jdbcConnectionTable;
  }

  /**
   * Method returns the connection for the request properties.
   *
   * @return
   * @throws Exception
   */
  public Connection getConnection(Properties properties) throws Exception {
    try {
      return this.connectionPool.borrowObject(properties);
    } catch (IOException | RuntimeException e) {
      e.printStackTrace();
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }

  /**
   * @param properties
   *
   * @return
   */
  private GenericKeyedObjectPoolConfig setAndGetPoolConfigProperties(Properties properties) {

    final GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

    boolean testOnBorrow = Boolean.valueOf(
        properties.getProperty(PoolProps.TEST_ON_BORROW.key,
            PoolProps.TEST_ON_BORROW.defValue));

    boolean testOnReturn = Boolean.valueOf(
        properties.getProperty(PoolProps.TEST_ON_RETURN.key,
            PoolProps.TEST_ON_RETURN.defValue));

    boolean testOnCreate = Boolean.valueOf(
        properties.getProperty(PoolProps.TEST_ON_CREATE.key,
            PoolProps.TEST_ON_CREATE.defValue));

    boolean testWhileIdle = Boolean.valueOf(
        properties.getProperty(PoolProps.TEST_WHILE_IDLE.key,
            PoolProps.TEST_WHILE_IDLE.defValue));

    poolConfig.setTestOnBorrow(testOnBorrow);
    poolConfig.setTestOnReturn(testOnReturn);
    poolConfig.setTestOnCreate(testOnCreate);
    poolConfig.setTestWhileIdle(testWhileIdle);

    int maxIdlePerKey = Integer.parseInt(
        properties.getProperty(PoolProps.MAX_IDLE_PER_KEY.key,
            PoolProps.MAX_IDLE_PER_KEY.defValue));
    poolConfig.setMaxIdlePerKey(maxIdlePerKey);

    int minIdlePerKey = Integer.parseInt(
        properties.getProperty(PoolProps.MIN_IDLE_PER_KEY.key,
            PoolProps.MIN_IDLE_PER_KEY.defValue));
    poolConfig.setMinIdlePerKey(minIdlePerKey);

    int maxTotalPerKey = Integer.parseInt(
        properties.getProperty(PoolProps.MAX_TOTAL_PER_KEY.key,
            PoolProps.MAX_TOTAL_PER_KEY.defValue));
    poolConfig.setMaxTotalPerKey(maxTotalPerKey);

    int maxTotal = Integer.parseInt(
        properties.getProperty(PoolProps.MAX_TOTAL.key,
            PoolProps.MAX_TOTAL.defValue));
    poolConfig.setMaxTotal(maxTotal);

    long maxWaitInMillis = Long.parseLong(
        properties.getProperty(PoolProps.MAX_WAIT_IN_MILLIS.key,
            PoolProps.MAX_WAIT_IN_MILLIS.defValue));
    poolConfig.setMaxWaitMillis(maxWaitInMillis);

    long timeBetweenEvictionRunMillis = Long.parseLong(
        properties.getProperty(PoolProps.TIME_BETWEEN_EVICTION_RUNS_MILLIS.key,
            PoolProps.TIME_BETWEEN_EVICTION_RUNS_MILLIS.defValue));
    poolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunMillis);

    long minEvictableIdleTimeMillis = Long.parseLong(
        properties.getProperty(PoolProps.MIN_EVICTABLE_IDLE_TIME_MILLIS.key,
            PoolProps.MIN_EVICTABLE_IDLE_TIME_MILLIS.defValue));
    poolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    poolConfig.setSoftMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

    boolean lifo = Boolean.valueOf(
        properties.getProperty(PoolProps.LIFO.key,
            PoolProps.LIFO.defValue));
    poolConfig.setLifo(lifo);

    boolean blockWhenExhausted = Boolean.valueOf(
        properties.getProperty(PoolProps.BLOCK_WHEN_EXHAUSTED.key,
            PoolProps.BLOCK_WHEN_EXHAUSTED.defValue));
    poolConfig.setBlockWhenExhausted(blockWhenExhausted);

    boolean fairness = Boolean.valueOf(
        properties.getProperty(PoolProps.FAIRNESS.key,
            PoolProps.FAIRNESS.defValue));
    poolConfig.setFairness(fairness);
   /*
    poolConfig.setJmxEnabled();
    poolConfig.setJmxNameBase();
    poolConfig.setJmxNamePrefix();
   */
    return poolConfig;
  }
}