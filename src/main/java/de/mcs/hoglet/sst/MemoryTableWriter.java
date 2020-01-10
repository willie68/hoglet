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
package de.mcs.hoglet.sst;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.utils.logging.Logger;

/**
 * @author wklaa_000
 *
 */
public class MemoryTableWriter implements Closeable {

  public static final byte[] ENTRY_START = "@".getBytes(StandardCharsets.UTF_8);
  public static final byte[] ENTRY_SEPERATOR = "#".getBytes(StandardCharsets.UTF_8);

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;
  private int level;
  private int number;
  private BloomFilter<MapKey> bloomfilter;
  private File sstFile;
  private RandomAccessFile raf;
  private FileChannel fileChannel;
  private long chunkCount;
  private VLogEntryInfo lastVLogEntry;

  /**
   * creating a new SS Table writer in the desired path.
   * 
   * @param options
   *          the options for the writing
   * @param level
   *          the level of this sst file
   * @param number
   *          the number of this sst file
   * @throws SSTException
   *           if something goes wrong
   * @throws IOException
   */
  public MemoryTableWriter(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    init();
  }

  private void init() throws SSTException, IOException {
    if (level < 0) {
      throw new SSTException("level should be greater or equal 0");
    }
    if (number < 0) {
      throw new SSTException("number should be greater or equal 0");
    }
    MapKeyFunnel funnel = new MapKeyFunnel();
    long keyCount = ((long) Math.pow(options.getLvlTableCount(), level)) * options.getMemTableMaxKeys();
    bloomfilter = BloomFilter.create(funnel, keyCount, 0.01);

    createSSTable();

    chunkCount = 0;
  }

  /**
   * creating a new SST in the path with the given level and number
   * 
   * @throws SSTException
   * @throws IOException
   */
  private void createSSTable() throws SSTException, IOException {
    sstFile = DatabaseUtils.getSSTFilePath(new File(options.getPath()), level, number);
    log.debug("creating new sst file: %s", sstFile.getName());
    if (sstFile.exists()) {
      throw new SSTException(String.format("sst file for level %d number %d already exists.", level, number));
    }

    raf = new RandomAccessFile(sstFile, "rw");
    raf.setLength(options.getVlogMaxSize());
    raf.seek(0);
    fileChannel = raf.getChannel();
  }

  /**
   * writes a single key/value to the file and add the key to the bloomfilter for this file.
   * 
   * @param entry
   *          the key/value to add
   * @throws IOException
   */
  public void write(Entry entry) throws IOException {
    bloomfilter.put(entry.getKey());
    // format of entry is start byte '@', length of entry, key, seperator '#',
    // value
    // length of entry is key, seperator '#', value
    int entryLength = 4 + entry.getKey().getKey().length + 1 + 1 + 1 + 4 + entry.getValue().length;
    int bufLength = 1 + 4 + entryLength;
    ByteBuffer bb = ByteBuffer.allocate(bufLength);
    bb.put(ENTRY_START);
    bb.putInt(entryLength);
    bb.putInt(entry.getKey().getKey().length);
    bb.put(entry.getKey().getKey());
    bb.put(ENTRY_SEPERATOR);
    bb.put((byte) entry.getOperation().ordinal());
    bb.put(ENTRY_SEPERATOR);
    bb.putInt(entry.getValue().length);
    bb.put(entry.getValue());
    bb.flip();

    fileChannel.write(bb);
    chunkCount++;
  }

  public boolean mightContain(MapKey key) {
    return bloomfilter.mightContain(key);
  }

  /**
   * writing the sst information structure and than closing the file,
   */
  @Override
  public void close() throws IOException {
    long position = fileChannel.position();

    // writing the bloomfilter and some statistics at the end of the file
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomfilter.writeTo(out);
    SSTStatus sstStatus = new SSTStatus().withBloomfilter(out.toByteArray()).withChunkCount(chunkCount)
        .withCreatedAt(new Date()).withLastVLogEntry(lastVLogEntry);
    fileChannel.write(StandardCharsets.UTF_8.encode(sstStatus.toJson()));
    ByteBuffer bb = ByteBuffer.allocate(128);
    bb.putLong(position);
    bb.flip();
    fileChannel.write(bb);
    // closing the file
    position = fileChannel.position();
    raf.setLength(position);
    fileChannel.force(true);
    fileChannel.close();
    raf.close();
  }

  /**
   * @return the lastVLogEntry
   */
  public VLogEntryInfo getLastVLogEntry() {
    return lastVLogEntry;
  }

  /**
   * @param lastVLogEntry
   *          the lastVLogEntry to set
   */
  public void setLastVLogEntry(VLogEntryInfo lastVLogEntry) {
    this.lastVLogEntry = lastVLogEntry;
  }

  /**
   * @return the sstFile
   */
  public File getSstFile() {
    return sstFile;
  }

}
