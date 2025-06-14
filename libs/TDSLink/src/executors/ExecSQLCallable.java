package executors;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

import constants.DateConstants;
import java.sql.SQLException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import pool.ConnectionPool;
import requests.SQLRequest;
import utils.EncodedLogger;

/**
 * A Callable implementation that executes SQL queries against a Sybase database
 * using connection pooling and returns results in JSON format.
 * 
 * <p>
 * Handles special conversion for date/time types to ensure consistent ISO-8601
 * formatting.
 * Provides logging capabilities for debugging purposes.
 * </p>
 * 
 * @author rod
 * @contributor DarkJ24
 * @contributor CatHood0
 */
public class ExecSQLCallable implements Callable<String> {

  private final ConnectionPool connectionPool;
  private final SQLRequest sqlRequest;

  /**
   * Constructs a new ExecSQLCallable instance.
   * 
   * @param connectionPool The connection pool to obtain database connections from
   * @param sqlRequest     The SQL request containing the query and metadata
   * @param shouldLog      Whether to enable logging of execution details
   */
  public ExecSQLCallable(ConnectionPool connectionPool, SQLRequest sqlRequest) {
    this.connectionPool = connectionPool;
    this.sqlRequest = sqlRequest;
  }

  /**
   * Executes the SQL request and returns the result as a JSON string.
   * 
   * @return JSON string containing query results or error information
   * @throws Exception if any error occurs during execution
   */
  @Override
  public String call() throws Exception {
    String jsonResult = executeSqlQuery();
    EncodedLogger.log("Query executed successfully. Result size: " + jsonResult.length());
    // the unique way to send the response back to the client
    // is printing it to the console here
    //
    // if we use Future clase with get() method,
    // the response wont be sent back to the client
    // or sometimes will convert the current thread in
    // a zombie one
    System.out.println(jsonResult);
    return jsonResult;
  }

  /**
   * Handles the SQL query execution and result processing.
   * 
   * @return JSON string containing the query results
   */
  private String executeSqlQuery() {
    final JSONObject response = new JSONObject();
    final JSONArray resultSetsArray = new JSONArray();

    response.put("messageId", sqlRequest.msgId);
    response.put("result", resultSetsArray);

    Statement statement = null;
    ResultSet resultSet = null;
    Connection connection = null;

    try {
      connection = connectionPool.getConnection();
      statement = connection.createStatement();
      EncodedLogger.log("Obtained connection from pool");
      boolean hasResults = statement.execute(sqlRequest.sql);
      EncodedLogger.log("Query executed. Has results: " + hasResults);

      while (hasResults || (statement.getUpdateCount() != -1)) {
        if (!hasResults) {
          hasResults = statement.getMoreResults();
          continue;
        }

        resultSet = statement.getResultSet();
        ResultSetMetaData metaData = resultSet.getMetaData();

        // Get column names
        final int columnCount = metaData.getColumnCount();
        final String[] columnNames = new String[columnCount + 1];
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
          columnNames[columnIndex] = metaData.getColumnLabel(columnIndex);
        }

        final JSONArray resultRows = new JSONArray();
        resultSetsArray.add(resultRows);

        while (resultSet.next()) {
          final JSONObject rowData = new JSONObject();
          resultRows.add(rowData);

          // Process each column in the row
          for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            final String columnName = columnNames[columnIndex];
            final Object columnValue = resultSet.getObject(columnIndex);

            // We prefer not to ignore this.
            //
            // Since when a value is NULL/null,
            // the query has likely been configured to
            // optionally have null fields
            if (columnValue == null) {
              rowData.put(columnName, null);
              continue;
            }

            int dataType = metaData.getColumnType(columnIndex);
            switch (dataType) {
              case DateConstants.TYPE_TIMESTAMP:
              case DateConstants.TYPE_DATE:
                final String date = DateConstants.DATE_FORMAT
                    .format(new Date(resultSet.getTimestamp(columnIndex).getTime()));
                rowData.put(columnName, date); // ISO "yyyy-MM-dd" format
                break;
              case DateConstants.TYPE_TIME:
                /**
                 * 
                 * String timeFromRS = rs.getTime(c).toString();
                 * String my8601formattedTime = "1970-01-01T" + timeFromRS + ".000Z";
                 * row.put(columns[c], my8601formattedTime);
                 */
                LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                rowData.put(columnName, time.format(DateTimeFormatter.ISO_TIME));
                break;
              default:
                rowData.put(columnName, columnValue);
            }
          }
        }
        resultSet.close();
        hasResults = statement.getMoreResults();
      }
      EncodedLogger.log("Closing connection with id=" + sqlRequest.id());
      statement.close();
      connection.close();
    } catch (SQLException ex) {
      response.put("error", ex.getMessage());
      EncodedLogger.logError("Error executing query");
      EncodedLogger.logException(ex);

      try {
        if (connection != null) {
          EncodedLogger.logError("Closing connection due to error");
          connection.close();
        }
      } catch (SQLException closingEx) {
        EncodedLogger.logError("Error closing connection with id=" + sqlRequest.id());
        EncodedLogger.logException(closingEx);
      }
    } finally {
      closeResource(resultSet, "result set");
      closeResource(statement, "statement");
    }

    response.put("javaStartTime", sqlRequest.javaStartTime);
    response.put("javaEndTime", System.currentTimeMillis());
    final String jsonResponse = response.toJSONString();
    return jsonResponse;
  }

  /**
   * Safely closes a database resource with error handling.
   * 
   * @param resource     The resource to close (ResultSet, Statement, etc.)
   * @param resourceName The name of the resource for logging purposes
   */
  private void closeResource(AutoCloseable resource, String resourceName) {
    if (resource == null)
      return;
    try {
      resource.close();
    } catch (Exception ex) {
      EncodedLogger.logError("Error closing " + resourceName);
      EncodedLogger.logException(ex);
    }
  }
}
