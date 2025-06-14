package utils;

public class EncodedLogger {
  public static Boolean log = true;
  private static final String ERROR_PREFIX = "JAVAERROR:";
  private static final String EXCEPTION_PREFIX = "JAVAEXCEPTION:";
  private static final String LOG_PREFIX = "JAVALOG:";

  private EncodedLogger() {
  }

  public static void logError(String str) {
    if (log) {
      String message = buildStr(ERROR_PREFIX, str, "");
      if (message.length() > 1000) {
        message = message.substring(0, 1000) + " ... (truncated)";
      }
      System.err.println(message);
    }
  }

  public static void logException(Exception ex) {
    if (log) {
      final String message = buildStr(EXCEPTION_PREFIX, "(1)", ex.toString(), "\n", EXCEPTION_PREFIX, "(2)",
          ex.getMessage());
      System.err.println(message);
    }
  }

  public static void log(String str) {
    if (log) {
      String message = buildStr(LOG_PREFIX, str);
      if (message.length() > 1000) {
        message = message.substring(0, 1000) + " ... (truncated)";
      }
      System.out.println(message);
    }
  }

  private static String buildStr(String... content) {
    String message = "";
    for (String str : content) {
      message += (str + " ");
    }
    return message.trim();
  }
}
