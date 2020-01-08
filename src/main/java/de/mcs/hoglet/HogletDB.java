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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import de.mcs.hoglet.event.WriteImmutableTableEventListener;
import de.mcs.hoglet.event.WriteImmutableTableEventListener.WriteImmutableTableEvent;
import de.mcs.hoglet.sst.Entry;
import de.mcs.hoglet.sst.MapKey;
import de.mcs.hoglet.sst.MemoryTable;
import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.sst.SSTableManager;
import de.mcs.hoglet.sst.SSTableReader;
import de.mcs.hoglet.sst.SortedMemoryTable;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.vlog.VLog;
import de.mcs.hoglet.vlog.VLogEntryDescription;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.hoglet.vlog.VLogList;
import de.mcs.utils.GsonUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class HogletDB implements Closeable {
  public final static int KEY_LENGTH = 255;
  public final static int COLLECTION_LENGTH = 255;

  private Logger log = Logger.getLogger(this.getClass());
  private static final String DEFAULT_COLLECTION = "default";
  private Options options;
  // TODO remove map implementation
  private HashMap<String, byte[]> map;

  private VLogList vLogList;
  private MemoryTable memoryTable;
  private MemoryTable immutableTable;
  private ReentrantLock immutableTableLock;
  private boolean readonly;
  private FileChannel channel;
  private FileLock writeLock;

  private DatabaseUtils databaseUtils;
  private EventBus eventBus;
  private SSTableManager ssTableManager;
  private ExecutorService eventBusExecutor;

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
    boolean newDB = true;
    // trying to check the path
    File dbFolder = new File(options.getPath());
    if (!dbFolder.exists()) {
      dbFolder.mkdirs();
    }
    File lockFile = new File(dbFolder, "LOCK");
    if (!lockFile.exists()) {
      try {
        lockFile.createNewFile();
      } catch (IOException e) {
        throw new HogletDBException("can't lock folder.", e);
      }
    }

    File optionFile = new File(dbFolder, "options.yml");
    if (optionFile.exists()) {
      newDB = false;
      try {
        String yaml = Files.readString(optionFile.toPath());
        Options fromYaml = Options.fromYaml(yaml);
        fromYaml.setPath(options.getPath());
        options = fromYaml;
      } catch (IOException e) {
        throw new HogletDBException("error reading options.yml.", e);
      }
    } else {
      try {
        Files.writeString(optionFile.toPath(), options.toYaml(), StandardOpenOption.CREATE_NEW);
      } catch (IOException e) {
        throw new HogletDBException("error writing options.yml.", e);
      }
    }

    File vlogFolder = new File(options.getVlogPath());
    if (newDB && !vlogFolder.exists()) {
      throw new HogletDBException("vlog folder doesn't exists for already created database.");
    }

    if (!vlogFolder.exists()) {
      vlogFolder.mkdirs();
    }

    try {
      channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      writeLock = channel.tryLock();
    } catch (OverlappingFileLockException e) {
      readonly = true;
    } catch (IOException e) {
      readonly = true;
    }
    vLogList = new VLogList(options);
    vLogList.setReadonly(readonly);
    memoryTable = getNewMemoryTable();
    immutableTable = null;
    immutableTableLock = new ReentrantLock();
    databaseUtils = DatabaseUtils.newDatabaseUtils(options);
    initEventbus();

    initSSTManager();

    replayVlog();
  }

  private void initSSTManager() throws HogletDBException {
    ssTableManager = SSTableManager.newSSTableManager(options, databaseUtils).withEventBus(eventBus);
  }

  private void replayVlog() throws HogletDBException {
    // TODO get the newest SST file, there from get the last VLogInfoEntry
    SSTableReader reader = ssTableManager.getNewestSST();
    if (reader != null) {
      // TODO this will be the starting point for the replay.
      VLogEntryInfo lastVLogEntry = reader.getLastVLogEntry();
      if (lastVLogEntry != null) {
        startReplay(lastVLogEntry);
      }
    }
  }

  private void startReplay(VLogEntryInfo lastVLogEntry) throws HogletDBException {
    int startVLogNumber = 0;
    long position = 0;
    if (lastVLogEntry != null) {
      startVLogNumber = DatabaseUtils.getVLogFileNumber(lastVLogEntry.getvLogName());
      position = lastVLogEntry.getStart();
    }
    List<File> vlogList = Arrays.asList(databaseUtils.getVLogFiles());
    vlogList.sort(new Comparator<File>() {

      @Override
      public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    boolean first = false;
    for (int i = 0; i < vlogList.size(); i++) {
      File vlogFile = vlogList.get(i);
      int number = DatabaseUtils.getVLogFileNumber(vlogFile.getName());
      if (number >= startVLogNumber) {
        try (VLog vLog = vLogList.getVLog(vlogFile.getName())) {
          for (Iterator<VLogEntryDescription> iterator = vLog.getIterator(position); iterator.hasNext();) {
            VLogEntryDescription entry = iterator.next();
            // first off all entries should be ignored, because the given Entry
            // is the last Entry which is saved to the file system.
            if (!first) {
              VLogEntryInfo info = VLogEntryInfo.newVLogEntryInfo().withStart(entry.getStart())
                  .withHash(entry.getHash()).withStartBinary(entry.getStartBinary()).withEnd(entry.getEnd())
                  .withVLogName(entry.getContainerName());
              memoryTable.add(entry.getCollection(), entry.getKey(), Operation.ADD,
                  info.asJson().getBytes(StandardCharsets.UTF_8));
            } else {
              first = false;
            }
          }
          position = 0;
        } catch (IOException e) {
          throw new HogletDBException(e);
        }
      }
    }
  }

  private void initEventbus() {
    eventBusExecutor = Executors.newSingleThreadExecutor();
    eventBus = new AsyncEventBus(eventBusExecutor);
    WriteImmutableTableEventListener writeImmutableTableEvent = new WriteImmutableTableEventListener(this);
    eventBus.register(writeImmutableTableEvent);
  }

  private SortedMemoryTable getNewMemoryTable() {
    return new SortedMemoryTable(options);
  }

  /**
   * testing if the db contains a key with the default collection.
   * 
   * @param key
   *          the key to test
   * @return true if the key is present in the database, otherwise false
   */
  public boolean contains(byte[] key) {
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
  public boolean contains(String collection, byte[] key) {
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
   * starting a new chunked add.
   * 
   * @param collection
   * @param key
   * @return ChunkList
   * @throws HogletDBException
   */
  public ChunkList createChunk(String collection, byte[] key) throws HogletDBException {
    if (isReadonly()) {
      throw new HogletDBException("hoglet is in readonly mode");
    }
    return ChunkList.newChunkList().withCollection(collection).withKey(key).withHogletDB(this);
  }

  private boolean containsKey(String collection, byte[] key) {
    if (memoryTable.containsKey(collection, key)) {
      Operation operation = memoryTable.getOperation(collection, key);
      if (Operation.DELETE.equals(operation)) {
        return false;
      }
      return true;
    }
    immutableTableLock.lock();
    try {
      if ((immutableTable != null) && immutableTable.containsKey(collection, key)) {
        Operation operation = immutableTable.getOperation(collection, key);
        if (Operation.DELETE.equals(operation)) {
          return false;
        }
        return true;
      }
    } finally {
      immutableTableLock.unlock();
    }

    for (ListIterator<SSTableReader> iterator = ssTableManager.iteratorInCreationOrder(); iterator.hasNext();) {
      SSTableReader ssTableReader = iterator.next();
      MapKey mapkey = MapKey.buildPrefixedKey(collection, key);
      try {
        Entry entry = ssTableReader.get(mapkey);
        if (entry != null) {
          if (Operation.DELETE.equals(entry.getOperation())) {
            return false;
          }
          return true;
        }
      } catch (IOException | SSTException e) {
        log.error("Error in SSTable", e);
      }
    }
    return false;
  }

  private byte[] getKey(String collection, byte[] key) throws HogletDBException {
    byte[] bs = memoryTable.get(collection, key);
    if (bs == null) {
      immutableTableLock.lock();
      try {
        if (immutableTable != null) {
          bs = immutableTable.get(collection, key);
        }
      } finally {
        immutableTableLock.unlock();
      }
    }

    if (bs == null) {
      for (ListIterator<SSTableReader> iterator = ssTableManager.iteratorInCreationOrder(); iterator.hasNext();) {
        SSTableReader ssTableReader = iterator.next();
        MapKey mapkey = MapKey.buildPrefixedKey(collection, key);
        try {
          if (ssTableReader.mightContain(mapkey)) {
            Entry entry = ssTableReader.get(mapkey);
            if (entry != null) {
              bs = entry.getValue();
            }
            if (bs != null) {
              break;
            }
          }
        } catch (IOException | SSTException e) {
          log.error("Error in SSTable", e);
        }
      }
    }
    if (bs == null) {
      return null;
    }

    VLogEntryInfo info = VLogEntryInfo.fromJson(new String(bs, StandardCharsets.UTF_8));
    if (info.getValue() != null) {
      return info.getValue();
    }
    try (VLog vLog = vLogList.getVLog(info.getvLogName())) {
      return vLog.getValue(info.getStartBinary(), info.getBinarySize());
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  private byte[] putKey(String collection, byte[] key, byte[] value) throws HogletDBException {
    if (isReadonly()) {
      throw new HogletDBException("hoglet is in readonly mode");
    }
    try {
      VLog vLog = vLogList.getNextAvailableVLog();
      // log.debug("putting into vlog file %s", vLog.getName());
      Operation operation = Operation.ADD;
      if (value.length < options.getDirectValueTreshHold()) {
        operation = Operation.ADD_DIRECT;
      }
      VLogEntryInfo info = vLog.put(collection, key, 0, value, operation);
      return putToMemoryTable(collection, key, operation, info);
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  private byte[] putToMemoryTable(String collection, byte[] key, Operation operation, VLogEntryInfo info)
      throws HogletDBException {
    byte[] tableValue = new byte[0];
    if (info != null) {
      tableValue = info.asJson().getBytes(StandardCharsets.UTF_8);
    }
    if (memoryTable.isAvailbleForWriting()) {
      return memoryTable.add(collection, key, operation, tableValue);
    }

    int count = 0;
    while ((immutableTable != null) && (count < 1000)) {
      // wait for writing of table...
      try {
        Thread.sleep(10);
      } catch (Exception e) {
      }
      count++;
    }
    if (immutableTable != null) {
      throw new HogletDBException("can't add value to db. db not ready...");
    }

    immutableTable = memoryTable;
    immutableTable.setLastVLogEntry(info);
    memoryTable = getNewMemoryTable();
    eventBus.post(new WriteImmutableTableEvent());

    return memoryTable.add(collection, key, operation, tableValue);
  }

  private byte[] removeKey(String collection, byte[] key) throws HogletDBException {
    if (isReadonly()) {
      throw new HogletDBException("hoglet is in readonly mode");
    }
    // check if key exists in db
    if (!keyMightExistsInTableBelow(collection, key)) {
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
    } else {
      try {
        VLog vLog = vLogList.getNextAvailableVLog();
        // log.debug("putting into vlog file %s", vLog.getName());
        Operation operation = Operation.DELETE;
        byte[] value = new byte[0];
        VLogEntryInfo info = vLog.put(collection, key, 0, value, operation);
        return putToMemoryTable(collection, key, operation, info);
      } catch (IOException e) {
        throw new HogletDBException(e);
      }
    }
  }

  private boolean keyMightExistsInTableBelow(String collection, byte[] key) {
    immutableTableLock.lock();
    try {
      if ((immutableTable != null) && immutableTable.containsKey(collection, key)) {
        return true;
      }
    } finally {
      immutableTableLock.unlock();
    }
    for (ListIterator<SSTableReader> iterator = ssTableManager.iteratorInCreationOrder(); iterator.hasNext();) {
      SSTableReader ssTableReader = iterator.next();
      MapKey mapkey = MapKey.buildPrefixedKey(collection, key);
      if (ssTableReader.mightContain(mapkey)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() throws HogletDBException {
    eventBusExecutor.shutdown();
    try {
      eventBusExecutor.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e1) {
      throw new HogletDBException("error shutting down eventbus", e1);
    }
    if (channel != null) {
      try {
        channel.close();
      } catch (IOException e) {
        throw new HogletDBException(e);
      }
    }
    if (writeLock != null) {
      try {
        if (writeLock.isValid()) {
          writeLock.close();
        }
      } catch (IOException e) {
        throw new HogletDBException(e);
      }
    }
    vLogList.close();
    try {
      ssTableManager.close();
    } catch (IOException e) {
      throw new HogletDBException(e);
    }
  }

  public InputStream getAsStream(String collection, byte[] key) throws HogletDBException {
    byte[] value = getKey(collection, key);
    ChunkList chunkList = GsonUtils.getJsonMapper().fromJson(new String(value, StandardCharsets.UTF_8),
        ChunkList.class);
    ChunkedInputStream inputStream = new ChunkedInputStream(getVLogList(), chunkList.getChunks());
    return inputStream;
  }

  public VLogList getVLogList() {
    return vLogList;
  }

  public boolean isReadonly() {
    return readonly;
  }

  public void writeImmutableTable() {
    if (immutableTable == null) {
      return;
    }
    try {
      ssTableManager.writeMemoryTable(immutableTable);
      immutableTable = null;
    } catch (IOException | SSTException e) {
      log.error("error writing immutable table", e);
    }
  }
}
