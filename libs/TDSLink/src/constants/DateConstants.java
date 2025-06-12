package constants;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateConstants {
  private DateConstants() {
  }

  public static void init() {
    if (!ISO_FORMATTER.getTimeZone().getID().equals("UTC")) {
      ISO_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
  }

  public static final int TYPE_TIMESTAMP = 93;
  public static final int TYPE_DATE = 91;
  public static final int TYPE_TIME = 92;
  public static final DateFormat ISO_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
}
