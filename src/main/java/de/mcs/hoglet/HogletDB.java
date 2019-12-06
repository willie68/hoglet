/**
 * Copyright 2019 w.klaas
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
package de.mcs.hoglet;

import java.io.Closeable;
import java.util.HashMap;

import de.mcs.utils.ByteArrayUtils;

/**
 * @author w.klaas
 *
 */
public class HogletDB implements Closeable {
  private static final String DEFAULT_COLLECTION = "default";
  private Options options;
  private HashMap<String, byte[]> map;

  /**
   * create a new instance of the hoglet key value store with specifig options.
   * 
   * @param options
   *          the options to set for the hoglet db
   */
  public HogletDB(Options options) {
    this.options = options;
    map = new HashMap<String, byte[]>();
  }

  /**
   * testing if the db contains a key with the default collection.
   * 
   * @param key
   *          the key to test
   * @return true if the key is present in the database, otherwise false
   */
  public boolean contains(byte[] key) {
    return containsKey(buildPrefixedKey(DEFAULT_COLLECTION, key));
  }

  /**
   * getting the value of the key in the default collection
   * 
   * @param key
   *          the key to the value
   * @return the value, if the key is not found, the value will be <code>null</code>
   */
  public byte[] get(byte[] key) {
    return getKey(buildPrefixedKey(DEFAULT_COLLECTION, key));
  }

  /**
   * putting a new key and value into the store in the default collection
   * 
   * @param key
   *          the key for the value
   * @param value
   *          the value to set
   * @return the value
   */
  public byte[] put(byte[] key, byte[] value) {
    return putKey(buildPrefixedKey(DEFAULT_COLLECTION, key), value);
  }

  /**
   * removes a key from the default collection
   * 
   * @param key
   *          the key to remove
   * @return the value
   */
  public byte[] remove(byte[] key) {
    return removeKey(buildPrefixedKey(DEFAULT_COLLECTION, key));
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public boolean contains(String collection, byte[] key) {
    return containsKey(buildPrefixedKey(collection, key));
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public byte[] get(String collection, byte[] key) {
    return getKey(buildPrefixedKey(collection, key));
  }

  /**
   * 
   * @param collection
   * @param key
   * @param value
   * @return
   */
  public byte[] put(String collection, byte[] key, byte[] value) {
    return putKey(buildPrefixedKey(collection, key), value);
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public byte[] remove(String collection, byte[] key) {
    return removeKey(buildPrefixedKey(collection, key));
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
  private byte[] buildPrefixedKey(String collection, byte[] key) {
    if (collection.contains(new String(new byte[] { 0 }))) {
      throw new IllegalArgumentException("collection name should not contain any null values.");
    }
    // todo create a faster key builder
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
    return buffer;
  }

  private boolean containsKey(byte[] key) {
    return map.containsKey(ByteArrayUtils.bytesAsHexString(key));
  }

  private byte[] getKey(byte[] key) {
    return map.get(ByteArrayUtils.bytesAsHexString(key));
  }

  private byte[] putKey(byte[] key, byte[] value) {
    return map.put(ByteArrayUtils.bytesAsHexString(key), value);
  }

  private byte[] removeKey(byte[] key) {
    return map.remove(ByteArrayUtils.bytesAsHexString(key));
  }

  @Override
  public void close() {
    // TODO nothing to do here, at the moment
  }
}
