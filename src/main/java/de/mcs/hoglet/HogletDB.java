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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import de.mcs.hoglet.vlog.VLog;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.hoglet.vlog.VLogList;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class HogletDB implements Closeable {
  private Logger log = Logger.getLogger(this.getClass());
  private static final String DEFAULT_COLLECTION = "default";
  private Options options;
  // TODO remove map implementation
  private HashMap<String, byte[]> map;

  private VLogList vLogList;
  private SortedMemoryTable memoryTable;

  /**
   * create a new instance of the hoglet key value store with specifig options.
   * 
   * @param options
   *          the options to set for the hoglet db
   * @throws HogletDBException
   */
  public HogletDB(Options options) throws HogletDBException {
    this.options = options;
    init();
  }

  private void init() throws HogletDBException {
    if (StringUtils.isEmpty(options.getPath())) {
      throw new HogletDBException("path should not be null or empty.");
    }
    // trying to check the path
    File dbFolder = new File(options.getPath());
    if (!dbFolder.exists()) {
      dbFolder.mkdirs();
    }
    // TODO remove map implementation
    vLogList = new VLogList(options);
    memoryTable = new SortedMemoryTable(options);
  }

  /**
   * testing if the db contains a key with the default collection.
   * 
   * @param key
   *          the key to test
   * @return true if the key is present in the database, otherwise false
   */
  public boolean contains(byte[] key) throws HogletDBException {
    return containsKey(DEFAULT_COLLECTION, key);
  }

  /**
   * getting the value of the key in the default collection
   * 
   * @param key
   *          the key to the value
   * @return the value, if the key is not found, the value will be <code>null</code>
   */
  public byte[] get(byte[] key) throws HogletDBException {
    return getKey(DEFAULT_COLLECTION, key);
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
  public byte[] put(byte[] key, byte[] value) throws HogletDBException {
    return putKey(DEFAULT_COLLECTION, key, value);
  }

  /**
   * removes a key from the default collection
   * 
   * @param key
   *          the key to remove
   * @return the value
   */
  public byte[] remove(byte[] key) throws HogletDBException {
    return removeKey(DEFAULT_COLLECTION, key);
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public boolean contains(String collection, byte[] key) throws HogletDBException {
    return containsKey(collection, key);
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public byte[] get(String collection, byte[] key) throws HogletDBException {
    return getKey(collection, key);
  }

  /**
   * 
   * @param collection
   * @param key
   * @param value
   * @return
   * @throws HogletDBException
   *           if something goes wrong
   */
  public byte[] put(String collection, byte[] key, byte[] value) throws HogletDBException {
    return putKey(collection, key, value);
  }

  /**
   * 
   * @param collection
   * @param key
   * @return
   */
  public byte[] remove(String collection, byte[] key) throws HogletDBException {
    return removeKey(collection, key);
  }

  /**
   * starting a new chunked add
   * 
   * @param collection
   * @param key
   * @return ChunkList
   */
  public ChunkList createChunk(String collection, byte[] key) {
    return ChunkList.newChunkList().withCollection(collection).withKey(key).withHogletDB(this);
  }

  private boolean containsKey(String collection, byte[] key) {
    return memoryTable.containsKey(collection, key);
  }

  private byte[] getKey(String collection, byte[] key) throws HogletDBException {
    byte[] bs = memoryTable.get(collection, key);
    if (bs == null) {
      return null;
    }
    VLogEntryInfo info = VLogEntryInfo.fromJson(new String(bs, StandardCharsets.UTF_8));
    try (VLog vLog = vLogList.getVLog(info.getvLogName())) {
      return vLog.getValue(info.getStartBinary(), info.getBinarySize());
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  private byte[] putKey(String collection, byte[] key, byte[] value) throws HogletDBException {
    try {
      VLog vLog = vLogList.getNextAvailableVLog();
      log.debug("putting into vlog file %s", vLog.getName());
      VLogEntryInfo info = vLog.put(collection, key, 0, value, Operation.ADD);

      return memoryTable.add(collection, key, info.asJson().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  private byte[] removeKey(String collection, byte[] key) throws HogletDBException {
    byte[] bs = memoryTable.remove(collection, key);
    if (bs == null) {
      return null;
    }
    VLogEntryInfo info = VLogEntryInfo.fromJson(new String(bs, StandardCharsets.UTF_8));
    try (VLog vLog = vLogList.getVLog(info.getvLogName())) {
      return vLog.getValue(info.getStartBinary(), info.getBinarySize());
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  @Override
  public void close() {
    vLogList.close();
  }

  public InputStream getAsStream(String collection, byte[] key) {
    return null;
  }

  public VLogList getVLogList() {
    return vLogList;
  }
}
