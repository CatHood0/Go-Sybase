package utils;

public class EncodedLogger {
  public static Boolean log;
  private static final String ERROR_PREFIX = "JAVAERROR:";
  private static final String EXCEPTION_PREFIX = "JAVAEXCEPTION:";
  private static final String LOG_PREFIX = "JAVALOG:";
  private static final StringBuffer buffer = new StringBuffer("");

  private EncodedLogger() {
  }

  public synchronized static void logError(String str) {
    buffer.delete(0, buffer.length());
    if (log) {
      String message = buildStr(ERROR_PREFIX, str, "");
      if (message.length() > 1000) {
        message = message.substring(0, 1000) + " ... (truncated)";
      }
      System.err.println(message.getBytes());
    }
  }

  public synchronized static void logException(Exception ex) {
    buffer.delete(0, buffer.length());
    if (log) {
      final String message = buildStr(EXCEPTION_PREFIX, "(1)", ex.toString(), "\n", EXCEPTION_PREFIX, "(2)",
          ex.getMessage());
      System.err.println(message.getBytes());
    }
  }

  public synchronized static void log(String str) {
    buffer.delete(0, buffer.length());
    if (log) {
      String message = buildStr(LOG_PREFIX, str);
      if (message.length() > 1000) {
        message = message.substring(0, 1000) + " ... (truncated)";
      }
      System.err.println(message.getBytes());
    }
  }

  private synchronized static String buildStr(String... content) {
    buffer.delete(0, buffer.length());
    for (String str : content) {
      buffer.append(str + " ");
    }
    return buffer.toString();
  }
}
