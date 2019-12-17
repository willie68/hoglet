/**
 * 
 */
package de.mcs.hoglet.sst;

import java.io.IOException;
import java.util.Map.Entry;

import de.mcs.hoglet.Options;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTCompacter {

  public static SSTCompacter newCompacter(Options options) {
    return new SSTCompacter(options);
  }

  private Options options;
  private int readingLevel;
  private int writingNumber;
  private Logger log = Logger.getLogger(this.getClass());

  public SSTCompacter(Options options) {
    this.options = options;
  }

  public SSTCompacter withReadingLevel(int level) {
    this.readingLevel = level;
    return this;
  }

  public SSTCompacter withWritingNumber(int writingNumber) {
    this.writingNumber = writingNumber;
    return this;
  }

  public void start() throws IOException, SSTException {
    // open all sst files from reading readingLevel
    try (SortedReaderQueue queue = SortedReaderQueue.newSortedReaderQueue(options).withReadingLevel(readingLevel)
        .open()) {
      try (MemoryTableWriter writer = new MemoryTableWriter(options, readingLevel + 1, writingNumber)) {

        while (queue.isAvailable()) {
          Entry<MapKey, byte[]> entry = queue.getNextEntry();
          if (entry != null) {
            writer.write(entry);
          }
        }
      }
    } catch (Exception e) {
      log.error(e);
    }
  }
}
