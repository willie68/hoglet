/**
 * 
 */
package de.mcs.hoglet.memorytable;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.hash.BloomFilter;

import de.mcs.hoglet.Options;

/**
 * @author w.klaas
 *
 */
public class SortedMemoryTable implements MemoryTable, Iterable<Entry<MapKey, byte[]>> {

  private Options options;
  private Map<MapKey, byte[]> map;
  private BloomFilter<MapKey> bloomfilter;
  private int missed;
  private int memsize;

  public SortedMemoryTable(Options options) {
    this.options = options;
    map = new TreeMap<>();
    // map = new HashMap<>(64 * 1024 * 1024);

    if (options.isMemActiveBloomFilter()) {
      MapKeyFunnel funnel = new MapKeyFunnel();
      bloomfilter = BloomFilter.create(funnel, 100000, 0.01);
    }
    missed = 0;
    memsize = 0;
  }

  /**
   * building the real key for the lsm tree store. A collection is only a prefix to the key.
   * 
   * @param collection
   *          the collection to set
   * @param key
   *          the key to the value
   * @return combination of collection and key
   */
  private static MapKey buildPrefixedKey(String collection, byte[] key) {

    byte[] colBytes = collection.getBytes(StandardCharsets.UTF_8);
    byte[] buffer = new byte[colBytes.length + key.length + 1];
    int x = 0;
    for (int i = 0; i < colBytes.length; i++) {
      buffer[x] = colBytes[i];
      x++;
    }
    buffer[x] = (byte) 0;
    x++;
    for (int i = 0; i < key.length; i++) {
      buffer[x] = key[i];
      x++;
    }
    return MapKey.wrap(buffer);
  }

  private void checkCollectionName(String collection) {
    if (collection.contains(new String(new byte[] { 0 }))) {
      throw new IllegalArgumentException("collection name should not contain any null values.");
    }
  }

  @Override
  public boolean containsKey(String collection, byte[] key) {
    checkCollectionName(collection);

    MapKey prefixedKey = buildPrefixedKey(collection, key);
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
  public byte[] get(String collection, byte[] key) {
    checkCollectionName(collection);
    MapKey prefixedKey = buildPrefixedKey(collection, key);

    if ((bloomfilter != null) && !bloomfilter.mightContain(prefixedKey)) {
      return null;
    }
    byte[] bs = map.get(prefixedKey);
    if (bs == null) {
      missed++;
    }
    return bs;
  }

  @Override
  public byte[] add(String collection, byte[] key, byte[] value) {
    checkCollectionName(collection);

    MapKey prefixedKey = buildPrefixedKey(collection, key);
    if (bloomfilter != null) {
      bloomfilter.put(prefixedKey);
    }
    memsize += prefixedKey.getKey().length + value.length + 40;
    return map.put(prefixedKey, value);
  }

  @Override
  public byte[] remove(String collection, byte[] key) {
    checkCollectionName(collection);
    MapKey prefixedKey = buildPrefixedKey(collection, key);
    return map.remove(prefixedKey);
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
  public Iterator<Entry<MapKey, byte[]>> iterator() {
    return map.entrySet().iterator();
  }
}
