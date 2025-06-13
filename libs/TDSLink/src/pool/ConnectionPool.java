package pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import utils.EncodedLogger;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A connection pool implementation using HikariCP for high-performance database
 * connection
 * pooling with Sybase databases.
 * 
 * <p>
 * This class provides efficient management of database connections with
 * configurable
 * pool sizing, timeouts, and lifecycle parameters.
 * </p>
 * 
 * @author DarkJ24
 * @contributor CatHood0
 */
public class ConnectionPool {

  private final HikariDataSource dataSource;

  /**
   * Creates a new ConnectionPool instance with the specified configuration.
   * 
   * @param host              The database server hostname
   * @param port              The database server port
   * @param dbName            The name of the database
   * @param username          Database username
   * @param password          Database password
   * @param minConnections    Minimum number of idle connections to maintain
   * @param maxConnections    Maximum number of connections in the pool
   * @param connectionTimeout Maximum time to wait for a connection (milliseconds)
   * @param idleTimeout       Maximum time a connection can sit idle
   *                          (milliseconds)
   * @param keepaliveTime     Time between keepalive checks (milliseconds)
   * @param maxLifetime       Maximum lifetime of a connection (milliseconds)
   * @param autoCommit        Whether connections should use auto-commit mode
   * @return Configured ConnectionPool instance
   * @throws SQLException If pool initialization fails
   */
  public static ConnectionPool create(
      String host, int port, String dbName, String username, String password,
      int minConnections, int maxConnections,
      int connectionTimeout, int idleTimeout,
      int keepaliveTime, int maxLifetime)
      throws SQLException {

    final HikariConfig config = createHikariConfig(
        host, port, dbName, username, password,
        minConnections, maxConnections,
        connectionTimeout, idleTimeout,
        keepaliveTime, maxLifetime);

    return new ConnectionPool(new HikariDataSource(config));
  }

  /**
   * Creates and configures a HikariConfig instance with the specified parameters.
   */
  private static HikariConfig createHikariConfig(
      String host, int port, String dbName, String username, String password,
      int minConnections, int maxConnections,
      int connectionTimeout, int idleTimeout,
      int keepaliveTime, int maxLifetime) {

    HikariConfig config = new HikariConfig();

    // Basic connection properties
    config.setDataSourceClassName("com.sybase.jdbc4.jdbc.SybDataSource");
    config.addDataSourceProperty("serverName", host);
    config.addDataSourceProperty("portNumber", port);
    config.addDataSourceProperty("databaseName", dbName);
    config.addDataSourceProperty("user", username);
    config.addDataSourceProperty("password", password);

    // Pool sizing configuration
    config.setMinimumIdle(minConnections);
    config.setMaximumPoolSize(maxConnections);

    // Timeout and lifecycle configuration
    config.setConnectionTimeout(connectionTimeout);
    config.setIdleTimeout(idleTimeout);
    config.setKeepaliveTime(keepaliveTime);
    config.setMaxLifetime(maxLifetime);

    // Transaction configuration
    config.setAutoCommit(true);

    return config;
  }

  /**
   * Constructs a new ConnectionPool with the given HikariDataSource.
   * 
   * @param dataSource Configured HikariDataSource instance
   */
  private ConnectionPool(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Retrieves a connection from the pool.
   * 
   * @return A database connection from the pool
   * @throws SQLException If no connection is available or connection fails
   */
  public Connection getConnection() throws SQLException {
    return this.dataSource.getConnection();
  }

  /**
   * Shuts down the connection pool, closing all active and idle connections.
   * 
   * @throws SQLException If any connections fail to close properly
   */
  public void shutdown() throws SQLException {
    try {
      this.dataSource.close();
    } catch (Exception e) {
      EncodedLogger.logError("Failed to shutdown connection pool" + e.toString());
    }
  }

  /**
   * Gets the current number of active connections in the pool.
   * 
   * @return Number of active connections
   */
  public int getActiveConnections() {
    return this.dataSource.getHikariPoolMXBean().getActiveConnections();
  }

  /**
   * Gets the current number of idle connections in the pool.
   * 
   * @return Number of idle connections
   */
  public int getIdleConnections() {
    return this.dataSource.getHikariPoolMXBean().getIdleConnections();
  }

  /**
   * Gets the total number of connections currently in the pool (active + idle).
   * 
   * @return Total number of connections
   */
  public int getTotalConnections() {
    return this.dataSource.getHikariPoolMXBean().getTotalConnections();
  }

  /**
   * Gets the number of threads awaiting connections from the pool.
   * 
   * @return Number of waiting threads
   */
  public int getThreadsAwaitingConnection() {
    return this.dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection();
  }
}
