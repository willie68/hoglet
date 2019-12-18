/**
 * 
 */
package de.mcs.hoglet.sst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import de.mcs.hoglet.Options;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SortedReaderQueue implements AutoCloseable {

  public static SortedReaderQueue newSortedReaderQueue(Options options) {
    return new SortedReaderQueue(options);
  }

  private class QueuedReader implements AutoCloseable {

    private Entry<MapKey, byte[]> nextEntry = null;
    private SSTableReader reader;
    private Iterator<Entry<MapKey, byte[]>> entries;

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

    public Entry<MapKey, byte[]> next() {
      if (nextEntry == null) {
        return null;
      }
      Entry<MapKey, byte[]> saveEntry = nextEntry;
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
  }

  private int readingLevel;
  private Options options;
  private List<QueuedReader> readerList;
  private Logger log = Logger.getLogger(this.getClass());
  private boolean deleteSource;

  public SortedReaderQueue(Options options) {
    this.readerList = new ArrayList<>();
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
    int number = 1;
    boolean found = true;
    while (found) {
      try {
        SSTableReader reader = new SSTableReaderMMF(options, readingLevel, number);
        readerList.add(new QueuedReader(reader));
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

  public Entry<MapKey, byte[]> getNextEntry() {
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
    return reader.next();
  }

}
