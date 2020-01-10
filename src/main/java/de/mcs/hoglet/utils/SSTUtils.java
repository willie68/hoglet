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
