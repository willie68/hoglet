/**
 * 
 */
package de.mcs.utils;

import java.util.Map;

/**
 * @author w.klaas
 *
 */
public class SystemHelper {
  private SystemHelper() {
  }

  public static String getComputerName() {
    Map<String, String> env = System.getenv();
    if (env.containsKey("COMPUTERNAME"))
      return env.get("COMPUTERNAME");
    else if (env.containsKey("HOSTNAME"))
      return env.get("HOSTNAME");
    else
      return "Unknown Computer";
  }
}
