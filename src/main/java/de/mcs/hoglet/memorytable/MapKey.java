/**
 * 
 */
package de.mcs.hoglet.memorytable;

import java.util.Arrays;

/**
 * @author w.klaas
 *
 */
public class MapKey implements Comparable<MapKey> {

  public static MapKey wrap(byte[] buffer) {
    MapKey mapKey = new MapKey();
    mapKey.key = Arrays.copyOf(buffer, buffer.length);
    return mapKey;
  }

  private byte[] key;

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MapKey)) {
      return false;
    }
    MapKey src = (MapKey) obj;
    return Arrays.equals(key, src.key);
  }

  @Override
  public int compareTo(MapKey o) {
    return Arrays.compare(key, o.key);
  }

  byte[] getKey() {
    return key;
  }
}
