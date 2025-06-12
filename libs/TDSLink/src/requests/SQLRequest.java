package requests;

/**
 * This class is used to store the information of a SQL request.
 * 
 * @author rod
 *         Modified by CatHood0
 */
public class SQLRequest {
  public int msgId; // The message id of the request
  public int transId; // The transaction id of the request
  public int timeout;
  public boolean finishTrans; // Indicates if the transaction needs to be finished
  public String timeoutUnit;
  public String sql; // The sql statement to be executed
  public long sentTime; // The time the request was sent
  public long javaStartTime; // The time the request was received

  @Override
  public String toString() {
    return String.format(
        "SQLRequest(msgId: %d, transId: %d, finishTrans: %b, sql: \"%s\", sentTime: %d, javaStartTime: %d, timeout: %d, timeoutUnit: \"%s\")",
        msgId,
        transId,
        finishTrans,
        sql,
        sentTime,
        javaStartTime,
        timeout,
        timeoutUnit);
  }
}
