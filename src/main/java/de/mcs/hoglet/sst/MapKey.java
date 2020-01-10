/**
 * Copyright 2020 w.klaas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  public String getCollection() {
    for (int i = 0; i < key.length; i++) {
      if (key[i] == 0) {
        return new String(Arrays.copyOf(key, i));
      }
    }
    return null;
  }

  public byte[] getKeyBytes() {
    for (int i = 0; i < key.length; i++) {
      if (key[i] == 0) {
        return Arrays.copyOfRange(key, i + 1, key.length);
      }
    }
    return null;
  }
}
