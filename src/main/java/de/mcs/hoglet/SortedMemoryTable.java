/**
 * 
 */
package de.mcs.hoglet;

import java.util.HashMap;
import java.util.Map;

import de.mcs.utils.ByteArrayUtils;

/**
 * @author w.klaas
 *
 */
public class SortedMemoryTable {

  private Options options;
  private Map<String, byte[]> map;

  public SortedMemoryTable(Options options) {
    this.options = options;
    map = new HashMap<String, byte[]>();
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
  private String buildPrefixedKey(String collection, byte[] key) {
    byte[] colBytes = collection.getBytes();
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
    return ByteArrayUtils.bytesAsHexString(buffer);
  }

  private void checkCollectionName(String collection) {
    if (collection.contains(new String(new byte[] { 0 }))) {
      throw new IllegalArgumentException("collection name should not contain any null values.");
    }
  }

  public boolean containsKey(String collection, byte[] key) {
    checkCollectionName(collection);
    return map.containsKey(buildPrefixedKey(collection, key));
  }

  public byte[] get(String collection, byte[] key) {
    checkCollectionName(collection);
    return map.get(buildPrefixedKey(collection, key));
  }

  public byte[] add(String collection, byte[] key, byte[] value) {
    checkCollectionName(collection);
    return map.put(buildPrefixedKey(collection, key), value);
  }

  public byte[] remove(String collection, byte[] key) {
    checkCollectionName(collection);
    return map.remove(buildPrefixedKey(collection, key));
  }

}
