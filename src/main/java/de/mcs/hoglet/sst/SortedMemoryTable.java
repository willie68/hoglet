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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.vlog.VLogEntryInfo;

/**
 * @author w.klaas
 *
 */
public class SortedMemoryTable implements MemoryTable, Iterable<Entry> {

  private static class MapEntry {
    Operation operation;
    byte[] value;

    static MapEntry newMapEntry() {
      return new MapEntry();
    }

    public MapEntry with(Operation operation) {
      this.operation = operation;
      return this;
    }

    public MapEntry with(byte[] value) {
      this.value = value;
      return this;
    }
  }

  private Options options;
  private Map<MapKey, MapEntry> map;
  private BloomFilter<MapKey> bloomfilter;
  private int missed;
  private int memsize;
  private VLogEntryInfo lastVLogEntry;

  public SortedMemoryTable(Options options) {
    this.options = options;
    map = new TreeMap<>();
    if (options.isMemActiveBloomFilter()) {
      MapKeyFunnel funnel = new MapKeyFunnel();
      bloomfilter = BloomFilter.create(funnel, 100000, 0.01);
    }
    missed = 0;
    memsize = 0;
  }

  private void checkCollectionName(String collection) {
    if (collection.contains(new String(new byte[] { 0 }))) {
      throw new IllegalArgumentException("collection name should not contain any null values.");
    }
  }

  @Override
  public boolean containsKey(String collection, byte[] key) {
    checkCollectionName(collection);

    MapKey prefixedKey = MapKey.buildPrefixedKey(collection, key);
    if ((bloomfilter != null) && !bloomfilter.mightContain(prefixedKey)) {
      return false;
    }
    boolean found = map.containsKey(prefixedKey);
    if (!found) {
      missed++;
    }
    return found;
  }

  @Override
  public Operation getOperation(String collection, byte[] key) {
    MapEntry mapEntry = getMapEntry(collection, key);
    if (mapEntry == null) {
      missed++;
      return null;
    }
    return mapEntry.operation;
  }

  private MapEntry getMapEntry(String collection, byte[] key) {
    checkCollectionName(collection);
    MapKey prefixedKey = MapKey.buildPrefixedKey(collection, key);

    if ((bloomfilter != null) && !bloomfilter.mightContain(prefixedKey)) {
      return null;
    }
    MapEntry mapEntry = map.get(prefixedKey);
    return mapEntry;
  }

  @Override
  public byte[] get(String collection, byte[] key) {
    MapEntry mapEntry = getMapEntry(collection, key);
    if (mapEntry == null) {
      missed++;
      return null;
    }
    byte[] bs = mapEntry.value;
    if (bs == null) {
      missed++;
    }
    return bs;
  }

  @Override
  public byte[] add(String collection, byte[] key, Operation operation, byte[] value) {
    checkCollectionName(collection);

    MapKey prefixedKey = MapKey.buildPrefixedKey(collection, key);
    if (bloomfilter != null) {
      bloomfilter.put(prefixedKey);
    }
    memsize += prefixedKey.getKey().length + 1 + value.length + 40;
    MapEntry mapEntry = MapEntry.newMapEntry().with(operation).with(value);
    map.put(prefixedKey, mapEntry);
    return value;
  }

  @Override
  public byte[] remove(String collection, byte[] key) {
    checkCollectionName(collection);
    MapKey prefixedKey = MapKey.buildPrefixedKey(collection, key);
    MapEntry mapEntry = map.remove(prefixedKey);
    return mapEntry.value;
  }

  @Override
  public int size() {
    return map.size();
  }

  /**
   * @return the missed
   */
  public int getMissed() {
    return missed;
  }

  @Override
  public boolean isAvailbleForWriting() {
    if ((options.getMemTableMaxKeys() > 0) && (map.size() >= options.getMemTableMaxKeys())) {
      return false;
    }
    if (memsize > options.getMemTableMaxSize()) {
      return false;
    }
    return true;
  }

  @Override
  public Iterator<Entry> iterator() {
    final Iterator<java.util.Map.Entry<MapKey, MapEntry>> iterator = map.entrySet().iterator();
    return new Iterator<Entry>() {

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Entry next() {
        java.util.Map.Entry<MapKey, MapEntry> next = iterator.next();
        return new Entry().withKey(next.getKey()).withValue(next.getValue().value)
            .withOperation(next.getValue().operation);
      }
    };
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

}
