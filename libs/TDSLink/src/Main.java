import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import constants.DateConstants;
import database_manager.SybaseDatabase;
import input_reader.StdInputReader;
import requests.SQLRequest;
import requests.SQLRequestListener;
import utils.EncodedLogger;

/**
 * Main entry point for the SQL Bridge application that:
 * 1. Accepts JSON-formatted SQL requests via stdin
 * 2. Executes them against a Sybase database
 * 3. Returns results as JSON via stdout
 * 
 * <p>
 * Message format:
 * Input: {"msgId": 1, "sql": "SELECT...", "timeout": 30, "timeunit": "seconds"}
 * Output: {"msgId": 1, "rows": [{},{}], "error": ""}
 * </p>
 */
public class Main implements SQLRequestListener {
  private static final int REQUIRED_ARGS = 13;
  private final SybaseDatabase db;
  private final StdInputReader input;
  private final Boolean log;

  /**
   * Application entry point.
   * 
   * @param args optional can be passed in the commandline arguments in order:
   *             host, port, dbname, username, password, log,
   *             minConnections, maxConnections, connectionTimeout,
   *             idleTimeout, keepaliveTime, maxLifetime, transactionConnections
   */
  public static void main(String[] args) {
    final Properties props = buildProperties(args);
    launchApplication(props);
  }

  /**
   * Validates command line arguments or loads from properties file if no args
   * provided.
   */
  private static Properties buildProperties(String[] args) {
    final Properties props = new Properties();
    if (args.length == 1) {
      final String tdslinkPath = args[0];
      final File configFile = new File(tdslinkPath);
      if (configFile.exists()) {
        try {
          props.load(new FileReader(configFile));
          return props;
        } catch (Exception e) {
          EncodedLogger.logError("Error reading tdslink.properties");
          EncodedLogger.logException(e);
          System.exit(1);
        }
      } else {
        EncodedLogger.logError("Invalid tdslink.properties path => \"" + tdslinkPath + "\"");
        System.exit(1);
      }
    }

    if (args.length != REQUIRED_ARGS) {
      EncodedLogger.logError("JAVAERROR: required args => <host>, <port>, <dbname>, <username>, <password>, <log>, " +
          "<minConnections>, <maxConnections>, <connectionTimeout>, <idleTimeout>, " +
          "<keepaliveTime>, <maxLifetime>, <transactionConnections>\n" +
          "Or provide a tdslink.properties file in the root directory with these properties.");
      System.exit(1);
    }

    // this means that we don't have a properties file
    props.setProperty("host", args[0]);
    props.setProperty("port", args[1]);
    props.setProperty("dbname", args[2]);
    props.setProperty("username", args[3]);
    props.setProperty("password", args[4]);
    props.setProperty("log", args[5]);
    props.setProperty("minConnections", args[6]);
    props.setProperty("maxConnections", args[7]);
    props.setProperty("connectionTimeout", args[8]);
    props.setProperty("idleTimeout", args[9]);
    props.setProperty("keepaliveTime", args[10]);
    props.setProperty("maxLifetime", args[11]);
    props.setProperty("transactionConnections", args[12]);
    return props;
  }

  /**
   * Initializes and launches the application.
   */
  private static void launchApplication(Properties properties) {
    try {
      new Main(properties);
    } catch (NumberFormatException e) {
      System.err.println("JAVAERROR: Invalid numeric argument: " + e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Constructs and initializes the SQL Bridge application.
   */
  public Main(Properties properties) {
    final int port = Integer.parseInt(properties.getProperty("port"));
    final int minConnections = Integer.parseInt(properties.getProperty("minConnections"));
    final int maxConnections = Integer.parseInt(properties.getProperty("maxConnections"));
    final int connectionTimeout = Integer.parseInt(properties.getProperty("connectionTimeout"));
    final int idleTimeout = Integer.parseInt(properties.getProperty("idleTimeout"));
    final int keepaliveTime = Integer.parseInt(properties.getProperty("keepaliveTime"));
    final int maxLifetime = Integer.parseInt(properties.getProperty("maxLifetime"));
    final int transactionConnections = Integer.parseInt(properties.getProperty("transactionConnections"));
    final String host = properties.getProperty("host");
    final String dbname = properties.getProperty("dbname");
    final String username = properties.getProperty("username");
    final String password = properties.getProperty("password");
    final Boolean log = Boolean.valueOf(properties.getProperty("log"));
    this.log = log;
    EncodedLogger.log = log;

    validateDatabaseCredentials(username, password);

    this.db = initializeDatabase(host, port, dbname, username, password, log,
        minConnections, maxConnections, connectionTimeout, idleTimeout,
        keepaliveTime, maxLifetime, transactionConnections);

    this.input = initializeInputReader(log);
    DateConstants.init();

    logConnectionSuccess(host, port, dbname);
    startInputProcessing();
  }

  /**
   * Validates basic database credentials.
   */
  private void validateDatabaseCredentials(String username, String password) {
    if (username == null || username.isEmpty() || password == null) {
      if (this.log) {
        System.err.println("JAVAERROR: Invalid database credentials");
      }
      System.exit(1);
    }
  }

  /**
   * Initializes the database connection pool.
   */
  private SybaseDatabase initializeDatabase(String host, int port, String dbname, String username,
      String password, boolean log, int minConnections,
      int maxConnections, int connectionTimeout, int idleTimeout,
      int keepaliveTime, int maxLifetime, int transactionConnections) {
    SybaseDatabase database = new SybaseDatabase(
        host, port, dbname, username, password,
        minConnections, maxConnections, connectionTimeout,
        idleTimeout, keepaliveTime, maxLifetime, transactionConnections);

    if (!database.connect()) {
      EncodedLogger.logError("Database isn't connected");
      System.exit(1);
    }
    return database;
  }

  /**
   * Initializes the input reader and registers this class as listener.
   */
  private StdInputReader initializeInputReader(boolean logEnabled) {
    StdInputReader reader = new StdInputReader();
    reader.addListener(this);
    return reader;
  }

  /**
   * Logs successful database connection.
   */
  private void logConnectionSuccess(String host, int port, String dbname) {
    EncodedLogger.log("Connected to " + host + ":" + port + "/" + dbname);
    EncodedLogger.log("Ready to process SQL requests");
  }

  /**
   * Starts the main input processing loop.
   */
  private void startInputProcessing() {
    input.startReadLoop();
  }

  /**
   * Handles incoming SQL requests by executing them against the database.
   * 
   * @param request The SQL request to execute
   */
  @Override
  public void sqlRequest(SQLRequest request) {
    if (request == null || request.sql == null || request.sql.trim().isEmpty()) {
      EncodedLogger.logError("Received invalid SQL request (It will be ignored)");
      return;
    }

    EncodedLogger.log("Processing request msgId = " + request.msgId);

    db.execSQL(request);
  }
}
