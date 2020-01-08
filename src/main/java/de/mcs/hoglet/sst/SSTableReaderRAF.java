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
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTableReaderRAF implements Closeable, SSTableReader {

  private class PositionEntry {
    long position;
    Entry entry;
    long nextPosition;
  }

  public class SSTableRAFIterator implements Iterator<Entry> {
    private SSTableReaderRAF reader;
    private PositionEntry next;

    public SSTableRAFIterator(SSTableReaderRAF reader) throws IOException, SSTException {
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
  private long[] indexList;
  private MapKey[] mapkeyList;
  private ReentrantLock readLock;
  private DatabaseUtils databaseUtils;
  private SSTStatus sstStatus;
  private long endOfSST;
  private SSTIdentity sstIdentity;

  public SSTableReaderRAF(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    this.indexList = new long[CACHE_SIZE];
    this.mapkeyList = new MapKey[CACHE_SIZE];
    Arrays.fill(this.indexList, 0);
    this.chunkCount = 0;
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
  }

  @Override
  public boolean mightContain(MapKey key) {
    return bloomfilter.mightContain(key);
  }

  @Override
  public boolean contains(MapKey mapKey) throws IOException, SSTException {
    if (mightContain(mapKey)) {
      int count = 0;
      long startPosition = 0;
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
        fileChannel.position(startPosition);
        while ((fileChannel.position() < fileChannel.size()) && (fileChannel.position() < endOfSST)) {
          long savePosition = fileChannel.position();
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

  private PositionEntry read(long position) throws IOException, SSTException {
    PositionEntry positionEntry = new PositionEntry();
    positionEntry.position = position;
    readLock.lock();
    try {
      fileChannel.position(positionEntry.position);
      positionEntry.entry = read();
      positionEntry.nextPosition = fileChannel.position();
      return positionEntry;
    } finally {
      readLock.unlock();
    }
  }

  private Entry read() throws IOException, SSTException {
    byte entryStart = raf.readByte();
    if (MemoryTableWriter.ENTRY_START[0] != entryStart) {
      throw new SSTException("error on sst file");
    }
    int entryLength = raf.readInt();
    ByteBuffer bb = ByteBuffer.allocate(entryLength);
    fileChannel.read(bb);
    bb.rewind();
    int value = bb.getInt();
    byte[] mapkeyBytes = new byte[value];
    bb.get(mapkeyBytes);

    MapKey mapKey = MapKey.wrap(mapkeyBytes);

    byte entrySeperator = bb.get();
    if (MemoryTableWriter.ENTRY_SEPERATOR[0] != entrySeperator) {
      throw new SSTException("error on sst file");
    }
    byte opByte = bb.get();
    Operation operation = Operation.values()[opByte];

    entrySeperator = bb.get();
    if (MemoryTableWriter.ENTRY_SEPERATOR[0] != entrySeperator) {
      throw new SSTException("error on sst file");
    }
    value = bb.getInt();
    byte[] valueBytes = new byte[value];
    bb.get(valueBytes);

    return new Entry().withKey(mapKey).withValue(valueBytes).withOperation(operation);
  }

  @Override
  public Entry get(MapKey mapKey) throws IOException, SSTException {
    if (mightContain(mapKey)) {
      int count = 0;
      long startPosition = 0;
      Monitor m = MeasureFactory.start("SSTableReaderRAF#get.lookup");
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
        m = MeasureFactory.start("SSTableReaderRAF#get.position");
        fileChannel.position(startPosition);
        m.stop();

        m = MeasureFactory.start("SSTableReaderRAF#get.scan");
        try {
          while ((fileChannel.position() < fileChannel.size()) && (fileChannel.position() < endOfSST)) {
            long savePosition = fileChannel.position();
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
    fileChannel.close();
    raf.close();
  }

  @Override
  public Iterator<Entry> entries() throws IOException, SSTException {
    return new SSTableRAFIterator(this);
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

}
