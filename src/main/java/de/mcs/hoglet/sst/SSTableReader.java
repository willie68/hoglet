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
import java.util.Map.Entry;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;
import de.mcs.utils.GsonUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTableReader implements Closeable {
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

  public SSTableReader(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
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

    chunkCount = 0;
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

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
