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
import java.util.List;

import de.mcs.hoglet.HogletDB;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.sst.SortedReaderQueue.QueuedReaderEntry;
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
  private HogletDB hogletDB;
  private int ignored;
  private int readingIncarnation;
  private int writingIncarnation;

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

  public SSTCompacter withReadingIncarnation(int incarnation) {
    this.readingIncarnation = incarnation;
    return this;
  }

  public SSTCompacter withWritingIncarnation(int incarnation) {
    this.writingIncarnation = incarnation;
    return this;
  }

  public SSTCompacter withHogletDB(HogletDB hogletDB) {
    this.hogletDB = hogletDB;
    return this;
  }

  public List<String> start() throws IOException, SSTException {
    // open all sst files from reading readingLevel
    try (SortedReaderQueue queue = SortedReaderQueue.newSortedReaderQueue(options).withReadingLevel(readingLevel)
        .open()) {
      int writingLevel = readingLevel + 1;
      SSTIdentity writingIdentity = SSTIdentity.newSSTIdentity().withLevel(writingLevel).withNumber(writingNumber)
          .withIncarnation(writingIncarnation);
      try (MemoryTableWriter writer = new MemoryTableWriter(options, writingIdentity)) {

        while (queue.isAvailable()) {
          QueuedReaderEntry entry = queue.getNextEntry();
          if (entry != null) {
            MapKey mapKey = entry.getEntry().getKey();
            if (!hogletDB.containsKeyUptoSST(mapKey.getCollection(), mapKey.getKey(), entry.getSstIdentity())) {
              writer.write(entry.getEntry());
            } else {
              ignored++;
            }
          }
        }
        writer.setLastVLogEntry(queue.getLastVLogEntry());
      }
      return queue.getTableNames();
    } catch (Exception e) {
      log.error(e);
    }
    return null;
  }

  public int getInored() {
    return ignored;
  }
}
