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
  private MappedByteBuffer mmfBuffer;
  ReentrantLock readLock;
  private DatabaseUtils databaseUtils;
  private SSTStatus sstStatus;

  public SSTableReaderMMF(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    this.indexList = new int[CACHE_SIZE];
    this.mapkeyList = new MapKey[CACHE_SIZE];
    Arrays.fill(this.indexList, 0);
    this.chunkCount = 0;
    this.readLock = new ReentrantLock();
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
    long statusPosition = bb.getLong();
    fileChannel.position(statusPosition);
    bb = ByteBuffer.allocate((int) (endStatus - statusPosition));
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
        while (mmfBuffer.position() < mmfBuffer.limit()) {
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
          while (mmfBuffer.position() < mmfBuffer.limit()) {
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
              return null;
            }
          }
        } finally {
          m.stop();
        }
      } finally {
        readLock.unlock();
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    MMFUtils.unMapBuffer(mmfBuffer, fileChannel.getClass());
    fileChannel.close();
    raf.close();
  }

  @Override
  public Iterator<Entry> entries() throws IOException, SSTException {
    return new SSTableMMFIterator(this);
  }

  @Override
  public VLogEntryInfo getLastVLogEntry() {
    return sstStatus.getLastVLogEntry();
  }

}
