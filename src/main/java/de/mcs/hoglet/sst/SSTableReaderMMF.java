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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.MMFUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTableReaderMMF implements Closeable, SSTableReader {

  private class PositionEntry {
    int position;
    Entry entry;
    int nextPosition;
  }

  public class SSTableMMFIterator implements Iterator<Entry> {
    private SSTableReaderMMF reader;
    private PositionEntry next;

    public SSTableMMFIterator(SSTableReaderMMF reader) throws IOException, SSTException {
      this.reader = reader;
      next = reader.read(0);
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Entry next() {
      PositionEntry actual = next;
      try {
        next = reader.read(next.nextPosition);
      } catch (IOException | SSTException e) {
        next = null;
      }
      return actual.entry;
    }
  }

  private static final int CACHE_SIZE = 100;
  private Logger log = Logger.getLogger(this.getClass());
  private Options options;
  private int level;
  private int number;
  private BloomFilter<MapKey> bloomfilter;
  private long chunkCount;
  private String filename;
  private File sstFile;
  private RandomAccessFile raf;
  private FileChannel fileChannel;
  private int[] indexList;
  private MapKey[] mapkeyList;
  private long missedKeys;
  private MappedByteBuffer mmfBuffer = null;
  ReentrantLock readLock;
  private DatabaseUtils databaseUtils;
  private SSTStatus sstStatus;
  private long endOfSST;
  private SSTIdentity sstIdentity;

  public SSTableReaderMMF(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    this.indexList = new int[CACHE_SIZE];
    this.mapkeyList = new MapKey[CACHE_SIZE];
    Arrays.fill(this.indexList, 0);
    this.chunkCount = 0;
    this.missedKeys = 0;
    this.readLock = new ReentrantLock();
    this.sstIdentity = SSTIdentity.newSSTIdentity().withNumber(number).withLevel(level);
    init();
  }

  private void init() throws SSTException, IOException {
    if (level < 0) {
      throw new SSTException("level should be greater than 0");
    }
    if (number < 0) {
      throw new SSTException("number should be greater than 0");
    }
    MapKeyFunnel funnel = new MapKeyFunnel();
    long keyCount = ((long) Math.pow(options.getLvlTableCount(), level - 1)) * options.getMemTableMaxKeys();
    bloomfilter = BloomFilter.create(funnel, keyCount, 0.01);

    databaseUtils = DatabaseUtils.newDatabaseUtils(options);
    openSSTable();

  }

  private void openSSTable() throws SSTException, IOException {
    filename = DatabaseUtils.getSSTFileName(level, number);
    log.debug("reading sst file: %s", filename);

    sstFile = databaseUtils.getSSTFilePath(level, number);
    if (!sstFile.exists()) {
      throw new SSTException(String.format("sst file for level %d number %d does not exists.", level, number));
    }

    raf = new RandomAccessFile(sstFile, "r");
    raf.seek(raf.length() - 8);
    fileChannel = raf.getChannel();
    ByteBuffer bb = ByteBuffer.allocate(8);
    fileChannel.read(bb);
    bb.rewind();
    long endStatus = raf.length() - 8;
    endOfSST = bb.getLong();
    fileChannel.position(endOfSST);
    bb = ByteBuffer.allocate((int) (endStatus - endOfSST));
    fileChannel.read(bb);
    bb.rewind();
    CharBuffer json = StandardCharsets.UTF_8.decode(bb);
    sstStatus = SSTStatus.fromJson(json.toString());
    chunkCount = sstStatus.getChunkCount();

    MapKeyFunnel funnel = new MapKeyFunnel();
    bloomfilter = BloomFilter.readFrom(new ByteArrayInputStream(sstStatus.getBloomfilter()), funnel);
    fileChannel.position(0);
    mmfBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
  }

  @Override
  public boolean mightContain(MapKey key) {
    return bloomfilter.mightContain(key);
  }

  @Override
  public boolean contains(MapKey mapKey) throws IOException, SSTException {
    if (mightContain(mapKey)) {
      int count = 0;
      int startPosition = 0;
      for (int i = 0; i < indexList.length; i++) {
        if (indexList[i] == 0) {
          break;
        }
        if (mapKey.compareTo(mapkeyList[i]) >= 0) {
          startPosition = indexList[i];
        }
      }
      readLock.lock();
      try {
        mmfBuffer.position(startPosition);
        while ((mmfBuffer.position() < mmfBuffer.limit()) && (mmfBuffer.position() < endOfSST)) {
          int savePosition = mmfBuffer.position();
          Monitor m = MeasureFactory.start(this, "readNextEntry");
          Entry entry = read();
          m.stop();
          int index = Math.round((count * CACHE_SIZE) / chunkCount);
          if (indexList[index] == 0) {
            indexList[index] = savePosition;
            mapkeyList[index] = entry.getKey();
          }
          count++;
          int compareTo = entry.getKey().compareTo(mapKey);
          if (compareTo == 0) {
            return true;
          }
          if (compareTo > 0) {
            return false;
          }
        }
      } finally {
        readLock.unlock();
      }
    }
    return false;
  }

  private PositionEntry read(int position) throws IOException, SSTException {
    PositionEntry positionEntry = new PositionEntry();
    positionEntry.position = position;
    readLock.lock();
    try {
      mmfBuffer.position(positionEntry.position);
      positionEntry.entry = read();
      positionEntry.nextPosition = mmfBuffer.position();
      return positionEntry;
    } finally {
      readLock.unlock();
    }
  }

  private Entry read() throws IOException, SSTException {
    byte entryStart = mmfBuffer.get();
    if (MemoryTableWriter.ENTRY_START[0] != entryStart) {
      throw new SSTException("error on sst file");
    }
    int entryLength = mmfBuffer.getInt();
    int value = mmfBuffer.getInt();
    byte[] mapkeyBytes = new byte[value];
    mmfBuffer.get(mapkeyBytes);

    MapKey mapKey = MapKey.wrap(mapkeyBytes);

    byte entrySeperator = mmfBuffer.get();
    if (MemoryTableWriter.ENTRY_SEPERATOR[0] != entrySeperator) {
      throw new SSTException("error on sst file");
    }
    byte opByte = mmfBuffer.get();
    Operation operation = Operation.values()[opByte];

    entrySeperator = mmfBuffer.get();
    if (MemoryTableWriter.ENTRY_SEPERATOR[0] != entrySeperator) {
      throw new SSTException("error on sst file");
    }
    value = mmfBuffer.getInt();
    byte[] valueBytes = new byte[value];
    mmfBuffer.get(valueBytes);

    return new Entry().withKey(mapKey).withValue(valueBytes).withOperation(operation);
  }

  @Override
  public Entry get(MapKey mapKey) throws IOException, SSTException {
    if (mightContain(mapKey)) {
      int count = 0;
      int startPosition = 0;
      Monitor m = MeasureFactory.start("SSTableReaderMMF#get.lookup");
      for (int i = 0; i < indexList.length; i++) {
        if (indexList[i] == 0) {
          break;
        }
        if (mapKey.compareTo(mapkeyList[i]) >= 0) {
          startPosition = indexList[i];
        }
      }
      m.stop();

      readLock.lock();
      try {
        mmfBuffer.position(startPosition);

        m = MeasureFactory.start("SSTableReaderMMF#get.scan");
        try {
          while ((mmfBuffer.position() < mmfBuffer.limit()) && (mmfBuffer.position() < endOfSST)) {
            int savePosition = mmfBuffer.position();
            Entry entry = read();
            int index = Math.round((count * CACHE_SIZE) / chunkCount);
            if (indexList[index] == 0) {
              indexList[index] = savePosition;
              mapkeyList[index] = entry.getKey();
            }
            count++;
            int compareTo = entry.getKey().compareTo(mapKey);
            if (compareTo == 0) {
              return entry;
            }
            if (compareTo > 0) {
              missedKeys++;
              return null;
            }
          }
        } finally {
          m.stop();
        }
        missedKeys++;
      } finally {
        readLock.unlock();
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    if (mmfBuffer != null) {
      MMFUtils.unMapBuffer(mmfBuffer, fileChannel.getClass());
    }
    if ((fileChannel != null) && fileChannel.isOpen()) {
      fileChannel.close();
    }
    if (raf != null) {
      raf.close();
    }
  }

  @Override
  public Iterator<Entry> entries() throws IOException, SSTException {
    return new SSTableMMFIterator(this);
  }

  @Override
  public VLogEntryInfo getLastVLogEntry() {
    return sstStatus.getLastVLogEntry();
  }

  @Override
  public String getTableName() {
    return filename;
  }

  @Override
  public SSTIdentity getSSTIdentity() {
    return sstIdentity;
  }

  @Override
  public long getMissed() {
    return missedKeys;
  }

}
