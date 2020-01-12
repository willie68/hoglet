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
package de.mcs.hoglet.sst;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.google.common.eventbus.EventBus;

import de.mcs.hoglet.HogletDB;
import de.mcs.hoglet.HogletDBException;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.event.CompactLevelEventListener;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.utils.SSTUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author wklaa_000
 *
 */
public class SSTableManager {

  /**
   * creating a new SSTable manager with the desired options
   * 
   * @param options
   *          the hoglet db options to use
   * @return a initialised table manager
   * @throws HogletDBException
   */
  public static SSTableManager newSSTableManager(Options options, DatabaseUtils databaseUtils)
      throws HogletDBException {
    SSTableManager tableManager = new SSTableManager().withOptions(options).withDatabaseUtils(databaseUtils);
    tableManager.initTableMatrix();
    return tableManager;
  }

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;
  private SSTableReader[][] tableMatrix;
  private DatabaseUtils databaseUtils;
  private EventBus eventBus;
  private HogletDB hogletDB;
  private int ignored;

  private SSTableManager() {
  }

  private void initTableMatrix() throws HogletDBException {
    tableMatrix = new SSTableReader[getOptions().getSSTMaxLevels()][getOptions().getLvlTableCount()];
    for (int i = 0; i < tableMatrix.length; i++) {
      for (int j = 0; j < tableMatrix[i].length; j++) {
        tableMatrix[i][j] = null;
      }
    }

    for (int level = 0; level < tableMatrix.length; level++) {
      File[] sstFiles = getDatabaseUtils().getSSTFiles(level);
      for (int number = 0; number < sstFiles.length; number++) {
        try {
          SSTableReader tableReader = SSTableReaderFactory.getReader(getOptions(), level, number);
          tableMatrix[level][number] = tableReader;
        } catch (SSTException | IOException e) {
          throw new HogletDBException(e);
        }
      }
    }
    log.info("init tables: \r\n%s", this.toString());
  }

  /**
   * @return the options
   */
  public Options getOptions() {
    return options;
  }

  /**
   * @param options
   *          the options to set
   */
  public void setOptions(Options options) {
    this.options = options;
  }

  /**
   * @param options
   *          the options to set
   * @return
   */
  public SSTableManager withOptions(Options options) {
    this.setOptions(options);
    return this;
  }

  /**
   * getting the newest sstable reader for the newest sst file
   * 
   * @return the newest sstablereader
   */
  public SSTableReader getNewestSST() {
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = tableMatrix[level].length - 1; number >= 0; number--) {
        SSTableReader ssTableReader = tableMatrix[level][number];
        if (ssTableReader != null) {
          return ssTableReader;
        }
      }
    }
    return null;
  }

  /**
   * getting an iterator over all sst files, ordered by creation, newest to latest
   * 
   * @return ListIterator<SSTableReader>
   */
  public ListIterator<SSTableReader> iteratorInCreationOrder() {
    List<SSTableReader> list = new ArrayList<>();
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = tableMatrix[level].length - 1; number >= 0; number--) {
        SSTableReader ssTableReader = tableMatrix[level][number];
        if (ssTableReader != null) {
          list.add(ssTableReader);
        }
      }
    }
    return list.listIterator();
  }

  /**
   * getting the next number of the sstable file
   * 
   * @param level
   * @return
   */
  public int getNextTableNumber(int level) {
    for (int number = 0; number < tableMatrix[level].length; number++) {
      SSTableReader ssTableReader = tableMatrix[level][number];
      if (ssTableReader == null) {
        return number;
      }
    }
    return -1;
  }

  /**
   * @return the databaseUtils
   */
  public DatabaseUtils getDatabaseUtils() {
    return databaseUtils;
  }

  /**
   * @param databaseUtils
   *          the databaseUtils to set
   */
  public void setDatabaseUtils(DatabaseUtils databaseUtils) {
    this.databaseUtils = databaseUtils;
  }

  /**
   * @param databaseUtils
   *          the databaseUtils to set
   */
  private SSTableManager withDatabaseUtils(DatabaseUtils databaseUtils) {
    this.setDatabaseUtils(databaseUtils);
    return this;
  }

  /**
   * writing out the memory table to the next level 0 sst file. If this is the 10's file, the compact event for level 0
   * will be thrown.
   * 
   * @param immutableTable
   *          the memeory table to write out
   * @throws IOException
   *           if something goes wrong on the file syytem
   * @throws SSTException
   *           if something goes wrong in the sst structure
   */
  public void writeMemoryTable(MemoryTable immutableTable) throws IOException, SSTException {
    int number = getNextTableNumber(0);
    File file = SSTUtils.writeMemoryTable(options, number, immutableTable);
    SSTableReader reader = SSTableReaderFactory.getReader(options, 0, number);
    tableMatrix[0][number] = reader;
    if ((number + 1) >= options.getLvlTableCount()) {
      getEventBus().post(new CompactLevelEventListener.CompactLevelEvent().withLevel(0));
    }
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = 0; number < tableMatrix[level].length; number++) {
        SSTableReader ssTableReader = tableMatrix[level][number];
        if (ssTableReader != null) {
          b.append("X");
        } else {
          b.append("0");
        }
      }
      b.append("\r\n");
    }
    return b.toString();
  }

  public SSTableManager withEventBus(EventBus eventBus) {
    setEventBus(eventBus);
    initEventBus();
    return this;
  }

  private void initEventBus() {
    CompactLevelEventListener CompactLevelEvent = new CompactLevelEventListener(this);
    eventBus.register(CompactLevelEvent);
  }

  /**
   * @return the eventBus
   */
  public EventBus getEventBus() {
    return eventBus;
  }

  /**
   * @param eventBus
   *          the eventBus to set
   */
  public void setEventBus(EventBus eventBus) {
    this.eventBus = eventBus;
  }

  public void compactLevel(final int level) {
    log.info("compacting level " + level);
    final int nextLevel = level + 1;
    int number = getNextTableNumber(nextLevel);
    SSTCompacter compacter = SSTCompacter.newCompacter(options).withReadingLevel(level).withWritingNumber(number)
        .withHogletDB(hogletDB);
    try {
      List<String> tableNames = compacter.start();
      log.debug(String.format("ignored keys: %d", compacter.getInored()));
      ignored += compacter.getInored();
      SSTableReader reader = SSTableReaderFactory.getReader(options, nextLevel, number);
      tableMatrix[nextLevel][number] = reader;

      tableNames.forEach(n -> removeTable(level, n));

      if ((number + 1) >= options.getLvlTableCount()) {
        getEventBus().post(new CompactLevelEventListener.CompactLevelEvent().withLevel(level + 1));
      }
    } catch (IOException | SSTException e) {
      log.error(String.format("error on compacting level %d", level), e);
    }
  }

  private void removeTable(int level, String name) {
    SSTableReader[] ssTableReaders = tableMatrix[level];
    for (int i = 0; i < ssTableReaders.length; i++) {
      SSTableReader ssTableReader = ssTableReaders[i];
      if ((ssTableReader != null) && (name.equals(ssTableReader.getTableName()))) {
        tableMatrix[level][i] = null;
        try {
          ssTableReader.close();
          ssTableReader.deleteUnderlyingFile();
        } catch (IOException e) {
          log.error("error closing sstable", e);
        } catch (SSTException e) {
          log.error("error closing sstable", e);
        }
      }
    }
  }

  public void close() throws IOException {
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = 0; number < tableMatrix[level].length; number++) {
        SSTableReader ssTableReader = tableMatrix[level][number];
        if (ssTableReader != null) {
          ssTableReader.close();
        }
      }
    }
  }

  public SSTableManager withHogletDB(HogletDB hogletDB) {
    this.hogletDB = hogletDB;
    return this;
  }

  public int getIgnored() {
    return ignored;
  }
}
