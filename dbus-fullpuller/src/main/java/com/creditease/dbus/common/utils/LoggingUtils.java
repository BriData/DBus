package com.creditease.dbus.common.utils;

import java.sql.SQLException;

import org.slf4j.Logger;

/**
 * A helper class for logging.
 */
public final class LoggingUtils {

  private LoggingUtils() { }

  /**
   * Log entire chain of SQLExceptions using old SQLException.getNextException
   * interface instead of new Throwable.getCause().
   */
  public static void logAll(Logger log, SQLException e) {
    logAll(log, null, e);
  }

  public static void logAll(Logger log, String message, SQLException e) {
    log.error(message == null ? "Top level exception: " : message, e);
    e = e.getNextException();
    int indx = 1;
    while (e != null) {
      log.error("Chained exception " + indx + ": ", e);
      e = e.getNextException();
      indx++;
    }
  }

//  public static void setDebugLevel() {
//    Logger.getLogger("com.creditease.dbus").setLevel(Level.DEBUG);
//    Logger.getLogger("com.creditease.dbus").setLevel(Level.DEBUG);
//  }
}

