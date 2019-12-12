/**
 * 
 */
package de.mcs.hoglet.sst;

import java.nio.charset.StandardCharsets;
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

  /**
   * building the real key for the lsm tree store. A collection is only a prefix to the key.
   * 
   * @param collection
   *          the collection to set
   * @param key
   *          the key to the value
   * @return combination of collection and key
   */
  public static MapKey buildPrefixedKey(String collection, byte[] key) {

    byte[] colBytes = collection.getBytes(StandardCharsets.UTF_8);
    byte[] buffer = new byte[colBytes.length + key.length + 1];
    int x = 0;
    for (int i = 0; i < colBytes.length; i++) {
      buffer[x] = colBytes[i];
      x++;
    }
    buffer[x] = (byte) 0;
    x++;
    for (int i = 0; i < key.length; i++) {
      buffer[x] = key[i];
      x++;
    }
    return MapKey.wrap(buffer);
  }

  private byte[] key;

  public byte[] getKey() {
    return key;
  }

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
}
