package org.apache.hadoop.hbase.a_wkl.util;

import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class Logger {

  private static final HashMap<Class<?>, org.slf4j.Logger> loggerMap = new HashMap<>();

  public static void info(Class<?> clazz, String s, Object... objs) {
    print("info", clazz, s, objs);
  }

  public static void error(Class<?> clazz, String s, Object... objs) {
    print("error", clazz, s, objs);
  }

  public static void warn(Class<?> clazz, String s, Object... objs) {
    print("warn", clazz, s, objs);
  }

  public static void debug(Class<?> clazz, String s, Object... objs) {
    print("debug", clazz, s, objs);
  }

  private static void print(String level, Class<?> clazz, String s, Object... objs) {
    org.slf4j.Logger logger = getLogger(clazz);
    s = "[##########] " + s;

    switch (level) {
      case "info":
        logger.info(s, objs);
        break;
      case "error":
        logger.error(s, objs);
        break;
      case "warn":
        logger.warn(s, objs);
        break;
      case "debug":
        logger.debug(s, objs);
    }
  }

  private static org.slf4j.Logger getLogger(Class<?> clazz) {
    if (!loggerMap.containsKey(clazz)) {
      loggerMap.put(clazz, LoggerFactory.getLogger(clazz));
    }

    return loggerMap.get(clazz);
  }
}
