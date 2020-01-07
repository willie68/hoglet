/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: SSTUtils.java
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
package de.mcs.hoglet.utils;

import java.io.File;
import java.io.IOException;

import de.mcs.hoglet.Options;
import de.mcs.hoglet.sst.MemoryTable;
import de.mcs.hoglet.sst.MemoryTableWriter;
import de.mcs.hoglet.sst.SSTException;

/**
 * @author wklaa_000
 *
 */
public class SSTUtils {
  /**
   * writing the memory table to the first level on the storage.
   * 
   * @param options
   *          the database options to use
   * @param number
   *          the number of the sst file
   * @param table
   *          the memory table to write out
   * @return
   * @throws IOException
   *           if something goes wrong on the file system
   * @throws SSTException
   *           if something goes wrong with the sst management
   */
  public static File writeMemoryTable(Options options, int number, MemoryTable table) throws IOException, SSTException {
    try (MemoryTableWriter writer = new MemoryTableWriter(options, 0, number)) {
      writer.setLastVLogEntry(table.getLastVLogEntry());
      table.forEach(entry -> {
        try {
          writer.write(entry);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      return writer.getSstFile();
    }
  }
}
