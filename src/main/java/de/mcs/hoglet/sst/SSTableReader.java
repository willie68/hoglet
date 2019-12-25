/**
 * 
 */
package de.mcs.hoglet.sst;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import de.mcs.hoglet.vlog.VLogEntryInfo;

/**
 * @author w.klaas
 *
 */
public interface SSTableReader extends Closeable {

  /**
   * check, if the key might be in the sst. If this is false, the key is not in the sst, but if this is true, the key
   * could be in
   * the sst file. The only gurantee is, on flase, that the key isn't in the file.
   * 
   * @param key
   *          key to check
   * @return true/false
   */
  boolean mightContain(MapKey key);

  /**
   * 
   * check, if the key is in the sst. The gurante is, on both true or false.
   * 
   * @param key
   *          key to check
   * @return true/false
   * @throws IOException
   * @throws SSTException
   */
  boolean contains(MapKey key) throws IOException, SSTException;

  /**
   * getting the entry for the key
   * 
   * @param key
   *          key to check
   * @return Entry<MapKey, byte[]>
   * @throws IOException
   * @throws SSTException
   */
  Entry get(MapKey key) throws IOException, SSTException;

  /**
   * starting a iterator, going thru all entries of this file.
   * 
   * @return Iterator
   * @throws IOException
   * @throws SSTException
   */
  Iterator<Entry> entries() throws IOException, SSTException;

  /**
   * getting the last vlog entry for this sst file.
   * 
   * @return VLogEntryInfo
   */
  VLogEntryInfo getLastVLogEntry();

}
