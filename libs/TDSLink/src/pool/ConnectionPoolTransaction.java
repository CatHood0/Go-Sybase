package pool;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.sybase.jdbc4.jdbc.SybDataSource;

import utils.EncodedLogger;

/**
 * A connection pool implementation specifically designed for managing database
 * connections
 * within transactions for Sybase databases.
 * 
 * <p>
 * This pool maintains both available connections and connections currently in
 * use by
 * transactions, with proper cleanup and resource management.
 * </p>
 * 
 * @author DarkJ24
 * @contributor CatHood0
 */
public class ConnectionPoolTransaction {

  private final String databaseUrl;
  private final Properties connectionProperties;
  private final int maxTransactionConnections;
  private final List<Connection> availableConnections;
  private final HashMap<Integer, Connection> activeTransactionConnections;

  /**
   * Factory method to create a new ConnectionPoolTransaction instance.
   * 
   * @param host                   The database server hostname
   * @param port                   The database server port
   * @param dbName                 The database name
   * @param username               Database username
   * @param password               Database password
   * @param transactionConnections Number of connections to maintain in the pool
   * @return Configured ConnectionPoolTransaction instance
   * @throws SQLException           If connection cannot be established
   * @throws ClassNotFoundException If JDBC driver not found
   */
  public static ConnectionPoolTransaction create(
      String host, int port, String dbName,
      String username, String password, int transactionConnections)
      throws SQLException, ClassNotFoundException {
    final String url = buildJdbcUrl(host, port, dbName);
    registerSybaseDriver();

    Properties props = buildProperties(username, password);
    List<Connection> initialConnections = initializeConnectionPool(url, props, transactionConnections);

    return new ConnectionPoolTransaction(url, props, initialConnections, transactionConnections);
  }

  /**
   * Builds the JDBC connection URL for Sybase.
   */
  private static String buildJdbcUrl(String host, int port, String dbName) {
    return "jdbc:sybase:Tds:" + host + ":" + port + "/" + dbName;
  }

  /**
   * Registers the Sybase JDBC driver.
   * 
   * @throws SQLException
   */
  private static void registerSybaseDriver() throws ClassNotFoundException, SQLException {
    try {
      SybDataSource sybDriver = (SybDataSource) Class.forName("com.sybase.jdbc4.jdbc.SybDataSource")
          .getDeclaredConstructor()
          .newInstance();
      DriverManager.registerDriver((Driver) sybDriver);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      EncodedLogger.logError("Failed to register Sybase driver");
      EncodedLogger.logException(e);
    }
  }

  /**
   * Creates connection properties with credentials.
   */
  private static Properties buildProperties(String username, String password) {
    Properties props = new Properties();
    props.put("user", username);
    props.put("password", password);
    return props;
  }

  /**
   * Initializes the connection pool with initial connections.
   */
  private static List<Connection> initializeConnectionPool(
      String url, Properties props, int initialSize) throws SQLException {
    List<Connection> pool = new ArrayList<>(initialSize);
    for (int i = 0; i < initialSize; i++) {
      pool.add(createNewConnection(url, props));
    }
    return pool;
  }

  /**
   * Creates a new database connection.
   */
  private static Connection createNewConnection(String url, Properties props) throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  /**
   * Constructs a new ConnectionPoolTransaction.
   * 
   * @param url                       Database connection URL
   * @param props                     Connection properties
   * @param initialConnections        Initial pool connections
   * @param maxTransactionConnections Maximum connections to maintain
   */
  private ConnectionPoolTransaction(String url, Properties props,
      List<Connection> initialConnections,
      int maxTransactionConnections) {
    this.databaseUrl = url;
    this.connectionProperties = props;
    this.availableConnections = new ArrayList<>(initialConnections);
    this.activeTransactionConnections = new HashMap<>();
    this.maxTransactionConnections = maxTransactionConnections;
  }

  /**
   * Retrieves a connection for the specified transaction.
   * 
   * @param transactionId The transaction identifier
   * @return A database connection
   * @throws SQLException If connection cannot be obtained
   */
  public synchronized Connection getConnection(int transactionId) throws SQLException {
    Connection connection = this.activeTransactionConnections.get(transactionId);

    if (connection == null) {
      connection = !this.availableConnections.isEmpty() ? this.availableConnections.remove(0)
          : createNewConnection(this.databaseUrl, this.connectionProperties);

      this.activeTransactionConnections.put(transactionId, connection);
    }

    return connection;
  }

  /**
   * Releases a connection from a transaction back to the pool.
   * 
   * @param transactionId The transaction identifier
   * @throws SQLException If connection cannot be released
   */
  public synchronized void releaseConnection(int transactionId) throws SQLException {
    Connection connection = this.activeTransactionConnections.remove(transactionId);

    if (connection == null)
      return;
    try {
      if (!connection.isClosed()) {
        connection.close();
      }
    } finally {
      // Always attempt to replenish the pool
      if (this.getAvailableConnectionCount() < this.maxTransactionConnections) {
        try {
          this.availableConnections.add(
              createNewConnection(this.databaseUrl, this.connectionProperties));
        } catch (SQLException ex) {
          EncodedLogger.logException(ex);
        }
      }
    }
  }

  /**
   * Shuts down the connection pool and closes all connections.
   * 
   * @throws SQLException If any connection cannot be closed
   */
  public synchronized void shutdown() {
    // Close available connections
    for (Connection conn : availableConnections) {
      try {
        if (!conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException ex) {
        EncodedLogger.logError("Failed to close available connection");
        EncodedLogger.logException(ex);
      }
    }
    availableConnections.clear();

    // Close active transaction connections
    for (Connection conn : activeTransactionConnections.values()) {
      try {
        if (!conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException ex) {
        EncodedLogger.logException(ex);
      }
    }
    activeTransactionConnections.clear();
  }

  /**
   * Gets the number of available connections in the pool.
   * 
   * @return Available connection count
   */
  public synchronized int getAvailableConnectionCount() {
    return availableConnections.size();
  }

  /**
   * Gets the number of active transaction connections.
   * 
   * @return Active connection count
   */
  public synchronized int getActiveConnectionCount() {
    return activeTransactionConnections.size();
  }
}
