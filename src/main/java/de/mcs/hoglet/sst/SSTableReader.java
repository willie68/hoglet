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
import java.util.Map.Entry;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.GsonUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTableReader implements Closeable {
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

  public SSTableReader(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    this.indexList = new long[CACHE_SIZE];
    this.mapkeyList = new MapKey[CACHE_SIZE];
    Arrays.fill(this.indexList, 0);
    this.chunkCount = 0;
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

    openSSTable();

  }

  private void openSSTable() throws SSTException, IOException {
    filename = String.format("sst_%02d_%02d.sst", level, number);
    log.debug("reading sst file: %s", filename);

    sstFile = new File(options.getPath(), filename);
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
    String jsonString = json.toString();
    SSTStatus sstStatus = GsonUtils.getJsonMapper().fromJson(jsonString, SSTStatus.class);
    chunkCount = sstStatus.getChunkCount();

    MapKeyFunnel funnel = new MapKeyFunnel();
    bloomfilter = BloomFilter.readFrom(new ByteArrayInputStream(sstStatus.getBloomfilter()), funnel);
    fileChannel.position(0);
  }

  public boolean mightContain(MapKey key) {
    return bloomfilter.mightContain(key);
  }

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

      fileChannel.position(startPosition);
      while (fileChannel.position() < fileChannel.size()) {
        long savePosition = fileChannel.position();
        Entry<MapKey, byte[]> entry = read();
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
    }
    return false;

  }

  public Entry<MapKey, byte[]> read() throws IOException, SSTException {
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
    value = bb.getInt();
    byte[] valueBytes = new byte[value];
    bb.get(valueBytes);

    return new Entry<MapKey, byte[]>() {

      @Override
      public byte[] setValue(byte[] value) {
        return null;
      }

      @Override
      public byte[] getValue() {
        return valueBytes;
      }

      @Override
      public MapKey getKey() {
        return mapKey;
      }
    };
  }

  public Entry<MapKey, byte[]> get(MapKey mapKey) throws IOException, SSTException {
    if (mightContain(mapKey)) {
      int count = 0;
      long startPosition = 0;
      Monitor m = MeasureFactory.start("SSTableReader#get.lookup");
      for (int i = 0; i < indexList.length; i++) {
        if (indexList[i] == 0) {
          break;
        }
        if (mapKey.compareTo(mapkeyList[i]) >= 0) {
          startPosition = indexList[i];
        }
      }
      m.stop();
      if (startPosition > 0) {
        m = MeasureFactory.start(String.format("SSTableReader#get.positionCount_%02d", startPosition));
        m.stop();
      }
      m = MeasureFactory.start("SSTableReader#get.position");
      fileChannel.position(startPosition);
      m.stop();

      m = MeasureFactory.start("SSTableReader#get.scan");
      try {
        while (fileChannel.position() < fileChannel.size()) {
          long savePosition = fileChannel.position();
          Entry<MapKey, byte[]> entry = read();
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
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
    raf.close();
  }

}
