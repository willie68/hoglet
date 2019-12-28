/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: SSTableManager.java
 * EMail: W.Klaas@gmx.de
 * Created: 28.12.2019 wklaa_000
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
package de.mcs.hoglet.sst;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import de.mcs.hoglet.HogletDBException;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.utils.SSTUtils;

/**
 * @author wklaa_000
 *
 */
public class SSTableManager {

  /**
   * creating a new SSTable manager with the desired options
   * @param options the hoglet db options to use
   * @return a initialised table manager
   * @throws HogletDBException 
   */
  public static SSTableManager newSSTableManager(Options options, DatabaseUtils databaseUtils)
      throws HogletDBException {
    SSTableManager tableManager = new SSTableManager().withOptions(options).withDatabaseUtils(databaseUtils);
    tableManager.initTableMatrix();
    return tableManager;
  }

  private Options options;
  private SSTableReader[][] tableMatrix;
  private DatabaseUtils databaseUtils;

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
  }

  /**
   * @return the options
   */
  public Options getOptions() {
    return options;
  }

  /**
   * @param options the options to set
   */
  public void setOptions(Options options) {
    this.options = options;
  }

  /**
   * @param options the options to set
   * @return 
   */
  public SSTableManager withOptions(Options options) {
    this.setOptions(options);
    return this;
  }

  /**
   * getting the newest sstable reader for the newest sst file
   * @return the newest sstablereader
   */
  public SSTableReader getNewestSST() {
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = tableMatrix[level].length - 1; number == 0; number++) {
        SSTableReader ssTableReader = tableMatrix[level][number];
        if (ssTableReader != null) {
          return ssTableReader;
        }
      }
    }
    return null;
  }

  /**
   * getting an iterator over all sst files, ordered by creation 
   * @return ListIterator<SSTableReader>
   */
  public ListIterator<SSTableReader> iteratorInCreationOrder() {
    List<SSTableReader> list = new ArrayList<>();
    for (int level = 0; level < tableMatrix.length; level++) {
      for (int number = 0; number < tableMatrix[level].length; number++) {
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
    return 0;
  }

  /**
   * @return the databaseUtils
   */
  public DatabaseUtils getDatabaseUtils() {
    return databaseUtils;
  }

  /**
   * @param databaseUtils the databaseUtils to set
   */
  public void setDatabaseUtils(DatabaseUtils databaseUtils) {
    this.databaseUtils = databaseUtils;
  }

  /**
   * @param databaseUtils the databaseUtils to set
   */
  private SSTableManager withDatabaseUtils(DatabaseUtils databaseUtils) {
    this.setDatabaseUtils(databaseUtils);
    return this;
  }

  /**
   * adding a freshly created 
   * @param i
   * @param number
   */
  public void addSSTable(int i, int number) {
    // TODO Auto-generated method stub

  }

  /**
   * writing out the memory table to the next level 0 sst file. If this is the 10's file, the compact event for level 0 will be thrown.
   * @param immutableTable the memeory table to write out
   * @throws IOException if something goes wrong on the file syytem
   * @throws SSTException if something goes wrong in the sst structure
   */
  public void writeMemoryTable(MemoryTable immutableTable) throws IOException, SSTException {
    int number = getNextTableNumber(0);
    File file = SSTUtils.writeMemoryTable(options, number, immutableTable);
    SSTableReader reader = SSTableReaderFactory.getReader(options, 0, number);
    tableMatrix[0][number] = reader;
  }
}
