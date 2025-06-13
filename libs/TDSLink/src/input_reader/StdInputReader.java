package input_reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import requests.*;
import utils.EncodedLogger;

/**
 * Reads and processes SQL requests from standard input in JSON format,
 * acting as a bridge between external processes and Java applications.
 * 
 * <p>
 * This class handles the following workflow:
 * 1. Reads JSON-formatted SQL requests from stdin
 * 2. Validates and parses the requests
 * 3. Notifies registered listeners about incoming requests
 * 4. Maintains consistent error logging format (JAVAERROR/JAVALOG)
 * </p>
 * 
 * @author rod
 * @contributor CatHood0
 *
 *              TODO: we need add more ways to execute other type responses
 *              (like requesting the number of active connections)
 */
public class StdInputReader {
  private final List<SQLRequestListener> listeners = new ArrayList<>();
  private final BufferedReader inputBuffer = new BufferedReader(new InputStreamReader(System.in));
  private final JSONParser jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

  /**
   * Constructs a new StdInputReader instance.
   * 
   * @param logEnabled Whether to enable debug logging
   */
  public StdInputReader() {
    EncodedLogger.log("StdInputReader initialized with logging enabled");
  }

  /**
   * Starts an infinite loop reading from stdin and processing requests.
   * 
   * <p>
   * The loop will continue until:
   * - The input stream is closed
   * - An unrecoverable IO error occurs
   * - The thread is interrupted
   * </p>
   */
  public void startReadLoop() {
    try {
      String inputLine;
      while ((inputLine = inputBuffer.readLine()) != null) {
        processInputLine(inputLine);
      }
    } catch (IOException ex) {
      EncodedLogger.logException(ex);
    } finally {
      EncodedLogger.log("Input reader loop terminated");
    }
  }

  /**
   * Processes a single line of input from stdin.
   */
  private void processInputLine(String inputLine) {
    // Normalize the input by removing unwanted whitespace
    String normalizedInput = normalizeInput(inputLine);
    EncodedLogger.log("Processing raw input: " + normalizedInput);

    try {
      SQLRequest request = parseRequest(normalizedInput);
      if (request != null) {
        notifyListeners(request);
      }
    } catch (Exception ex) {
      EncodedLogger.logException(ex);
    }
  }

  /**
   * Normalizes the input string by:
   * - Removing newline escapes
   * - Trimming whitespace
   * - Removing spaces outside quoted strings
   */
  private String normalizeInput(String input) {
    return input.replaceAll("\\n", "\n")
        .trim()
        .replaceAll("\\s+(?=([^\"]*\"[^\"]*\")*[^\"]*$)", "");
  }

  /**
   * Parses and validates a JSON string into an SQLRequest object.
   * 
   * @param jsonString The JSON input string
   * @return Valid SQLRequest object or null if parsing fails
   */
  private SQLRequest parseRequest(String jsonString) {
    long startTime = System.currentTimeMillis();

    try {
      JSONObject json = (JSONObject) jsonParser.parse(jsonString);

      // Validate required fields
      if (!json.containsKey("msgId") || !json.containsKey("sql")) {
        System.err.println("JAVAERROR: Missing required fields [msgId or sql]");
        return null;
      }

      final SQLRequest request = new SQLRequest();
      request.javaStartTime = startTime;
      request.msgId = getIntValue(json, "msgId", 1);
      request.sql = json.get("sql").toString();
      request.transId = getIntValue(json, "transId", -1);
      request.finishTrans = getBooleanValue(json, "finishTrans", true);
      request.timeout = getIntValue(json, "timeout", 3);
      request.timeoutUnit = getStringValue(json, "timeunit", "minutes");
      return request;
    } catch (ParseException ex) {
      EncodedLogger.logException(ex);
      return null;
    } catch (Exception ex) {
      EncodedLogger.logException(ex);
      return null;
    }
  }

  /**
   * Helper method to safely extract integer values from JSON.
   */
  private int getIntValue(JSONObject json, String key, int defaultValue) {
    try {
      return json.get(key) != null ? ((Number) json.get(key)).intValue() : defaultValue;
    } catch (Exception ex) {
      EncodedLogger.logError("Invalid value for " + key + ", using default");
      return defaultValue;
    }
  }

  /**
   * Helper method to safely extract boolean values from JSON.
   */
  private boolean getBooleanValue(JSONObject json, String key, boolean defaultValue) {
    try {
      return json.get(key) != null ? (Boolean) json.get(key) : defaultValue;
    } catch (Exception ex) {
      EncodedLogger.logError("Invalid value for " + key + ", using default");
      return defaultValue;
    }
  }

  /**
   * Helper method to safely extract string values from JSON.
   */
  private String getStringValue(JSONObject json, String key, String defaultValue) {
    try {
      return json.get(key) != null ? json.get(key).toString() : defaultValue;
    } catch (Exception ex) {
      EncodedLogger.logError("Invalid value for " + key + ", using default");
      return defaultValue;
    }
  }

  /**
   * Notifies all registered listeners about a new SQL request.
   */
  private void notifyListeners(SQLRequest request) {
    if (listeners.isEmpty()) {
      EncodedLogger.logError("No listeners registered to handle request");
      return;
    }

    for (SQLRequestListener listener : listeners) {
      try {
        listener.sqlRequest(request);
      } catch (Exception ex) {
        EncodedLogger.logException(ex);
      }
    }
  }

  /**
   * Registers a new listener to receive SQL requests.
   * 
   * @param listener The listener to add
   * @return true if added, false if already registered
   */
  public boolean addListener(SQLRequestListener listener) {
    if (listener == null || listeners.contains(listener)) {
      return false;
    }
    listeners.add(listener);
    return true;
  }

  /**
   * Unregisters a listener.
   * 
   * @param listener The listener to remove
   * @return true if removed, false if not found
   */
  public boolean removeListener(SQLRequestListener listener) {
    return listeners.remove(listener);
  }

  /**
   * Gets the number of currently registered listeners.
   */
  public int getListenerCount() {
    return listeners.size();
  }
}
