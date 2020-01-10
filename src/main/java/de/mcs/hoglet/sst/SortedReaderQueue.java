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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import de.mcs.hoglet.Options;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SortedReaderQueue implements AutoCloseable {

  public static SortedReaderQueue newSortedReaderQueue(Options options) {
    return new SortedReaderQueue(options);
  }

  public static class QueuedReaderEntry {
    private SSTIdentity sstIdentity;
    private Entry entry;

    /**
     * @return the sstIdentity
     */
    public SSTIdentity getSstIdentity() {
      return sstIdentity;
    }

    /**
     * @param sstIdentity
     *          the sstIdentity to set
     */
    public void setSstIdentity(SSTIdentity sstIdentity) {
      this.sstIdentity = sstIdentity;
    }

    /**
     * @return the entry
     */
    public Entry getEntry() {
      return entry;
    }

    /**
     * @param entry
     *          the entry to set
     */
    public void setEntry(Entry entry) {
      this.entry = entry;
    }

    public static QueuedReaderEntry newQueuedReaderEntry() {
      return new QueuedReaderEntry();
    }

    public QueuedReaderEntry withEntry(Entry entry) {
      setEntry(entry);
      return this;
    }

    public QueuedReaderEntry withSSTIdentity(SSTIdentity identity) {
      setSstIdentity(identity);
      return this;
    }
  }

  private class QueuedReader implements AutoCloseable {

    private Entry nextEntry = null;
    private SSTableReader reader;
    private Iterator<Entry> entries;

    public QueuedReader(SSTableReader reader) throws IOException, SSTException {
      this.reader = reader;
      entries = reader.entries();
      if (entries.hasNext()) {
        nextEntry = entries.next();
      }
    }

    public boolean isAvailble() {
      return nextEntry != null;
    }

    public MapKey getNextKey() {
      if (nextEntry == null) {
        return null;
      }
      return nextEntry.getKey();
    }

    public Entry next() {
      if (nextEntry == null) {
        return null;
      }
      Entry saveEntry = nextEntry;
      if (entries.hasNext()) {
        nextEntry = entries.next();
      } else {
        nextEntry = null;
      }
      return saveEntry;
    }

    public void close() throws IOException {
      reader.close();
    }

    public SSTIdentity getSSTIdentity() {
      return reader.getSSTIdentity();
    }
  }

  private int readingLevel;
  private Options options;
  private List<QueuedReader> readerList;
  private Logger log = Logger.getLogger(this.getClass());
  private boolean deleteSource;
  private VLogEntryInfo lastVLogEntry;
  private List<String> tableNames;

  public SortedReaderQueue(Options options) {
    this.readerList = new ArrayList<>();
    this.tableNames = new ArrayList<>();
    this.options = options;
  }

  @Override
  public void close() throws Exception {
    if (readerList != null) {
      for (QueuedReader ssTableReader : readerList) {
        try {
          ssTableReader.close();
        } catch (Exception e) {
          log.error(e);
        }
      }
    }
  }

  public SortedReaderQueue withReadingLevel(int readingLevel) {
    this.readingLevel = readingLevel;
    return this;
  }

  public SortedReaderQueue open() throws IOException {
    tableNames.clear();
    int number = 0;
    boolean found = true;
    while (found) {
      try {
        SSTableReader reader = SSTableReaderFactory.getReader(options, readingLevel, number);
        tableNames.add(reader.getTableName());
        readerList.add(new QueuedReader(reader));
        lastVLogEntry = reader.getLastVLogEntry();
        number++;
      } catch (SSTException e) {
        found = false;
      }
    }
    return this;
  }

  public boolean isAvailable() {
    for (QueuedReader queuedReader : readerList) {
      if (queuedReader.isAvailble()) {
        return true;
      }
    }
    return false;
  }

  public QueuedReaderEntry getNextEntry() {
    MapKey nextEntry = null;
    int nextIndex = 0;
    for (int i = 0; i < readerList.size(); i++) {
      QueuedReader reader = readerList.get(i);
      if (reader.isAvailble()) {
        if (nextEntry == null) {
          nextEntry = reader.getNextKey();
          nextIndex = i;
        } else {
          if (nextEntry.compareTo(reader.getNextKey()) >= 0) {
            nextEntry = reader.getNextKey();
            nextIndex = i;
          }
        }
      }
    }
    QueuedReader reader = readerList.get(nextIndex);
    SSTIdentity identity = reader.getSSTIdentity();
    return QueuedReaderEntry.newQueuedReaderEntry().withEntry(reader.next()).withSSTIdentity(identity);
  }

  public VLogEntryInfo getLastVLogEntry() {
    return lastVLogEntry;
  }

  public List<String> getTableNames() {
    return Collections.unmodifiableList(tableNames);
  }

}
