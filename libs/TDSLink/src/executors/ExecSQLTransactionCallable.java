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
import pool.ConnectionPoolTransaction;
import requests.SQLRequest;
import utils.EncodedLogger;

/**
 * A Callable implementation for executing SQL queries within transactions.
 * 
 * <p>
 * This class handles transactional SQL operations, including proper connection
 * management,
 * transaction rollback on errors, and conversion of database results to JSON
 * format with
 * special handling for date/time types.
 * </p>
 * 
 * @author DarkJ24
 * @contributor CatHood0
 */
public class ExecSQLTransactionCallable implements Callable<String> {

  private final ConnectionPoolTransaction connectionPool;
  private final SQLRequest sqlRequest;

  /**
   * Constructs a new ExecSQLTransactionCallable instance.
   * 
   * @param connectionPool The transactional connection pool to use
   * @param sqlRequest     The SQL request containing query and transaction
   *                       details
   * @param shouldLog      Whether to enable execution logging
   */
  public ExecSQLTransactionCallable(ConnectionPoolTransaction connectionPool,
      SQLRequest sqlRequest) {
    this.connectionPool = connectionPool;
    this.sqlRequest = sqlRequest;
  }

  /**
   * Executes the SQL transaction and returns the result as JSON.
   * 
   * @return JSON string containing query results or error information
   * @throws Exception if any error occurs during execution
   */
  @Override
  public String call() throws Exception {
    String result = executeSqlTransaction();
    EncodedLogger.log("Transaction executed. Result size: " + result.length());
    // the unique way to send the response back to the client
    // is printing it to the console here
    //
    // if we use Future clase with get() method,
    // the response wont be sent back to the client
    // or sometimes will convert the current thread in
    // a zombie one
    System.out.println(result);
    return result;
  }

  /**
   * Executes the SQL transaction and formats the results as JSON.
   * 
   * @return JSON string containing the transaction results
   */
  public String executeSqlTransaction() {
    JSONObject response = new JSONObject();
    response.put("messageId", sqlRequest.msgId);
    response.put("transactionId", sqlRequest.transId);

    JSONArray resultSets = new JSONArray();
    response.put("result", resultSets);

    Statement statement = null;
    ResultSet resultSet = null;
    Connection connection = null;

    try {
      connection = this.connectionPool.getConnection(sqlRequest.transId);

      if (connection == null || connection.isClosed()) {
        response.put("error", "Transaction connection is closed");
        EncodedLogger.log("Connection is unavailable (null or closed)");
        return response.toJSONString();
      }
      EncodedLogger.log("Transaction connection established");

      statement = connection.createStatement();
      boolean hasResults = statement.execute(sqlRequest.sql);

      while (hasResults || (statement.getUpdateCount() != -1)) {
        if (!hasResults) {
          hasResults = statement.getMoreResults();
          continue;
        }

        resultSet = statement.getResultSet();
        final ResultSetMetaData metaData = resultSet.getMetaData();

        // Process column names
        final int columnCount = metaData.getColumnCount();
        final String[] columnNames = new String[columnCount + 1];
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
          columnNames[columnIndex] = metaData.getColumnLabel(columnIndex);
        }

        final JSONArray resultRows = new JSONArray();
        resultSets.add(resultRows);

        while (resultSet.next()) {
          final JSONObject rowData = new JSONObject();
          resultRows.add(rowData);

          // Process each column value
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
      statement.close();
    } catch (SQLException ex) {
      handleTransactionError(response, connection, ex);
    } finally {
      cleanupResources(resultSet, statement, connection);
    }

    response.put("javaStartTime", sqlRequest.javaStartTime);
    response.put("javaEndTime", System.currentTimeMillis());
    return response.toJSONString();
  }

  /**
   * Handles transaction errors including rollback and connection cleanup.
   * 
   * @param response   The JSON response object to populate with error details
   * @param connection The database connection to rollback
   * @param exception  The exception that occurred
   */
  private void handleTransactionError(JSONObject response,
      Connection connection,
      Exception exception) {
    response.put("error", exception.getMessage());

    try {
      if (connection != null) {
        EncodedLogger.log("Initiating transaction rollback");
        connection.rollback();
      }
    } catch (SQLException rollbackEx) {
      EncodedLogger.logError("Rollback failed");
      EncodedLogger.logException(rollbackEx);
    }

    EncodedLogger.logError("Transaction error caused by: ");
    EncodedLogger.logException(exception);
  }

  /**
   * Cleans up database resources and releases the connection if needed.
   * 
   * @param resultSet  The ResultSet to close
   * @param statement  The Statement to close
   * @param connection The Connection to potentially release
   */
  private void cleanupResources(ResultSet resultSet,
      Statement statement,
      Connection connection) {
    // Release connection back to pool if transaction is complete
    try {
      if (sqlRequest.finishTrans) {
        this.connectionPool.releaseConnection(sqlRequest.transId);
      }
    } catch (SQLException ex) {
      EncodedLogger.logError("Failed to release connection to pool");
      EncodedLogger.logException(ex);
    }

    // Close result set
    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException ex) {
      EncodedLogger.logError("Failed to close result set");
      EncodedLogger.logException(ex);
    }

    // Close statement
    try {
      if (statement != null) {
        statement.close();
      }
    } catch (SQLException ex) {
      EncodedLogger.logError("Failed to close statement");
      EncodedLogger.logException(ex);
    }
  }
}
