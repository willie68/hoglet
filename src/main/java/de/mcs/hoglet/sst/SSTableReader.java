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

  /**
   * 
   * @return the name of the underlying table file.
   */
  String getTableName();

  /**
   * 
   * @return the identity of this reader.
   */
  SSTIdentity getSSTIdentity();

  /**
   * getting a count of missed readings.
   * @return long
   */
  long getMissed();
}
