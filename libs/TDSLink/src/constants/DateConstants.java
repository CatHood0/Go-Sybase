package constants;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateConstants {
  private DateConstants() {
  }

  public static void init() {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public static final int TYPE_TIMESTAMP = 93;
  public static final int TYPE_DATE = 91;
  public static final int TYPE_TIME = 92;
  // Warning: This is not a standard SQL type, but used in TDS protocol
  // and works fine by now
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
}
