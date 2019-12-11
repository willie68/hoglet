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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;

/**
 * @author wklaa_000
 *
 */
public class MemoryTableWriter implements Closeable {

  private Options options;
  private int level;
  private int number;
  private BloomFilter<MapKey> bloomfilter;

  /**
   * creating a new SS Table writer in the desired path.
   * 
   * @param options
   *          the options for the writing
   * @param level
   * @param number
   */
  public MemoryTableWriter(Options options, int level, int number) {
    this.options = options;
    this.level = level;
    this.number = number;
    init();
  }

  private void init() {
    MapKeyFunnel funnel = new MapKeyFunnel();
    long keyCount = ((long) Math.pow(10, level)) * options.getMemTableMaxKeys();
    bloomfilter = BloomFilter.create(funnel, keyCount, 0.01);
  }

  /**
   * creating a new SST in the path with the given level and number
   * 
   * @param level
   * @param number
   */
  private void createSSTable(int level, int number) {

  }

  /**
   * writes a single key/value to the file and add the key to the bloomfilter for this file.
   * 
   * @param entry
   *          the key/value to add
   */
  public void write(Entry<MapKey, byte[]> entry) {
    bloomfilter.put(entry.getKey());
  }

  @Override
  public void close() throws IOException {
    // TODO close all desired files
    // writing the bloomfilter and some statistics at the end of the file
    // closing the file
  }

}
