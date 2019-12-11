/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: MemoryTableWriter.java
 * EMail: W.Klaas@gmx.de
 * Created: 10.12.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.hoglet.memorytable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author wklaa_000
 *
 */
public class MemoryTableWriter implements Closeable {

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;
  private int level;
  private int number;
  private BloomFilter<MapKey> bloomfilter;
  private File sstFile;
  private String filename;
  private RandomAccessFile raf;
  private FileChannel fileChannel;
  private long chunkCount;

  /**
   * creating a new SS Table writer in the desired path.
   * 
   * @param options
   *          the options for the writing
   * @param level the level of this sst file
   * @param number the number of this sst file
   * @throws SSTException  if something goes wrong
   * @throws IOException 
   */
  public MemoryTableWriter(Options options, int level, int number) throws SSTException, IOException {
    this.options = options;
    this.level = level;
    this.number = number;
    init();
  }

  private void init() throws SSTException, IOException {
    if (level <= 0) {
      throw new SSTException("level should be greater than 0");
    }
    if (number <= 0) {
      throw new SSTException("number should be greater than 0");
    }
    MapKeyFunnel funnel = new MapKeyFunnel();
    long keyCount = ((long) Math.pow(options.getLvlTableCount(), level - 1)) * options.getMemTableMaxKeys();
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
    filename = String.format("sst_%2d_%2d.sst", level, number);
    log.debug("creating new vlog file: %s", filename);

    sstFile = new File(options.getPath(), filename);
    if (sstFile.exists()) {
      throw new SSTException(String.format("sst file for level %d number %d already exists.", level, number));
    }

    raf = new RandomAccessFile(sstFile, "rw");
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
  public void write(Entry<MapKey, byte[]> entry) throws IOException {
    bloomfilter.put(entry.getKey());
    // format of entry is start byte '@', length of entry, key, seperator '#',
    // value
    // length of entry is key, seperator '#', value
    int entryLength = entry.getKey().getKey().length + 1 + entry.getValue().length;
    int bufLength = 1 + 8 + entryLength;
    ByteBuffer bb = ByteBuffer.allocate(bufLength);
    bb.putChar('@');
    bb.putInt(entryLength);
    bb.put(entry.getKey().getKey());
    bb.putChar('#');
    bb.put(entry.getValue());
    bb.flip();

    fileChannel.write(bb);
  }

  @Override
  public void close() throws IOException {
    // TODO close all desired files
    // writing the bloomfilter and some statistics at the end of the file
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomfilter.writeTo(out);
    String bloomString = ByteArrayUtils.bytesAsHexString(out.toByteArray());
    log.debug("Bloomfilter: %s", bloomString);
    // closing the file
    fileChannel.force(true);
    fileChannel.close();
    raf.close();
  }

}
