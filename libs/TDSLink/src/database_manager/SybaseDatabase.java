package database_manager;

import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import executors.ExecSQLCallable;
import executors.ExecSQLTransactionCallable;
import pool.ConnectionPool;
import pool.ConnectionPoolTransaction;
import requests.SQLRequest;

/**
 * Main database access class for Sybase databases, providing connection
 * pooling,
 * transaction management, and asynchronous query execution capabilities.
 *
 * <p>
 * This class manages two separate connection pools (regular and transactional)
 * and provides thread-safe execution of SQL queries with configurable timeouts.
 * </p>
 *
 * @author rod
 * @contributor DarkJ24
 * @contributor CatHood0
 */
public class SybaseDatabase {
  /**
   * Default number of threads in the execution pool
   */
  public static final int NUMBER_OF_THREADS = 10;

  // Database connection parameters
  private final String host;
  private final int port;
  private final String dbName;
  private final String username;
  private final String password;
  private final boolean logEnabled;

  // Connection pool configurations
  private final int minConnections;
  private final int maxConnections;
  private final int connectionTimeout;
  private final int idleTimeout;
  private final int keepaliveTime;
  private final int maxLifetime;
  private final int transactionConnections;

  // Connection pools
  private ConnectionPool pool;
  private ConnectionPoolTransaction transactionPool;

  // Thread pool for asynchronous execution
  private final ExecutorService executor;

  /**
   * Constructs a new SybaseDatabase instance with the specified configuration.
   *
   * @param host                   Database server hostname
   * @param port                   Database server port
   * @param dbName                 Database name
   * @param username               Database username
   * @param password               Database password
   * @param logEnabled             Whether to enable logging
   * @param minConnections         Minimum number of connections in the pool
   * @param maxConnections         Maximum number of connections in the pool
   * @param connectionTimeout      Connection timeout in milliseconds
   * @param idleTimeout            Idle timeout in milliseconds
   * @param keepaliveTime          Keepalive interval in milliseconds
   * @param maxLifetime            Maximum connection lifetime in milliseconds
   * @param transactionConnections Number of dedicated transaction connections
   */
  public SybaseDatabase(String host, int port, String dbName, String username, String password, boolean logEnabled,
      int minConnections, int maxConnections, int connectionTimeout, int idleTimeout,
      int keepaliveTime, int maxLifetime, int transactionConnections) {
    this.host = host;
    this.port = port;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.logEnabled = logEnabled;
    this.minConnections = minConnections;
    this.maxConnections = maxConnections;
    this.connectionTimeout = connectionTimeout;
    this.idleTimeout = idleTimeout;
    this.keepaliveTime = keepaliveTime;
    this.maxLifetime = maxLifetime;
    this.transactionConnections = transactionConnections;
    this.executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
  }

  /**
   * Establishes connection to the database and initializes connection pools.
   *
   * @return true if connection was successful, false otherwise
   */
  public boolean connect() {
    try {
      // Initialize regular connection pool
      this.pool = ConnectionPool.create(
          this.host, this.port, this.dbName, this.username, this.password,
          this.minConnections, this.maxConnections,
          this.connectionTimeout, this.idleTimeout,
          this.keepaliveTime, this.maxLifetime);

      // Initialize transactional connection pool
      this.transactionPool = ConnectionPoolTransaction.create(
          this.host, this.port, this.dbName,
          this.username, this.password, this.transactionConnections);

      // Register shutdown hook for proper resource cleanup
      registerShutdownHook();
      return true;

    } catch (Exception ex) {
      logException("Failed to connect to database", ex);
      return false;
    }
  }

  /**
   * Registers a shutdown hook to clean up resources when the JVM exits.
   */
  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        if (pool != null) {
          pool.shutdown();
        }
        if (transactionPool != null) {
          transactionPool.shutdown();
        }
        executor.shutdown();
      } catch (Exception ex) {
        logException("Error during shutdown", ex);
      }
    }));
  }

  /**
   * Executes an SQL query asynchronously with the specified timeout.
   *
   * @param request The SQL request to execute
   * @throws IllegalStateException if called before connecting to the database
   */
  public void execSQL(SQLRequest request) {
    if (pool == null || transactionPool == null) {
      throw new IllegalStateException("Database connection not established. Call connect() first.");
    }

    if (logEnabled) {
      System.out.println("JAVALOG: Executing request at " + LocalDate.now() + ". " + request);
    }

    Future<String> future = executor.submit(createCallable(request));
    awaitResult(future, request);
  }

  /**
   * Creates the appropriate callable based on request type.
   */
  private Callable<String> createCallable(SQLRequest request) {
    return request.transId != -1 ? new ExecSQLTransactionCallable(transactionPool, request, logEnabled)
        : new ExecSQLCallable(pool, request, logEnabled);
  }

  /**
   * Waits for the query result with the specified timeout.
   */
  private void awaitResult(Future<String> future, SQLRequest request) {
    try {
      System.out.println(future.get(request.timeout, parseTimeUnit(request.timeoutUnit)));
    } catch (TimeoutException e) {
      logException("Query timed out", e);
    } catch (InterruptedException e) {
      logException("Query execution interrupted", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      logException("Query execution failed", e);
    }
  }

  /**
   * Parses time unit string into TimeUnit enum.
   */
  private TimeUnit parseTimeUnit(String timeUnitStr) {
    switch (timeUnitStr.toLowerCase()) {
      case "seconds":
        return TimeUnit.SECONDS;
      case "minutes":
        return TimeUnit.MINUTES;
      case "hours":
        return TimeUnit.HOURS;
      case "days":
        return TimeUnit.DAYS;
      case "nanoseconds":
        return TimeUnit.NANOSECONDS;
      case "microseconds":
        return TimeUnit.MICROSECONDS;
      default:
        return TimeUnit.MILLISECONDS;
    }
  }

  /**
   * Closes all database connections and releases resources.
   */
  public void disconnect() {
    try {
      if (pool != null) {
        pool.shutdown();
      }
      if (transactionPool != null) {
        transactionPool.shutdown();
      }
      executor.shutdown();
    } catch (Exception ex) {
      logException("Error during disconnection", ex);
    }
  }

  /**
   * Logs an exception if logging is enabled.
   *
   * @param context Context description of the error
   * @param ex      The exception to log
   */
  private void logException(String context, Exception ex) {
    if (logEnabled) {
      System.err.println("JAVAERROR: (1) " + context + " => " + ex.toString());
      System.err.println("JAVAERROR: (2) " + context + " => " + ex.getMessage());
      ex.printStackTrace();
    }
  }
}
