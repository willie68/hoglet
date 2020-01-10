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
package de.mcs.hoglet.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.ListIterator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.mcs.hoglet.HogletDBException;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.sst.Entry;
import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.sst.SSTableManager;
import de.mcs.hoglet.sst.SSTableReader;
import de.mcs.utils.JsonByteArraySerializer;

/**
 * @author w.klaas
 *
 */
public class SSTExporter {

  private Options options;
  private DatabaseUtils databaseUtils;
  private SSTableManager ssTableManager;

  public SSTExporter(Options options) throws HogletDBException {
    this.options = options;
    databaseUtils = DatabaseUtils.newDatabaseUtils(options);

    initSSTManager();
  }

  private void initSSTManager() throws HogletDBException {
    ssTableManager = SSTableManager.newSSTableManager(options, databaseUtils);
  }

  public void export(OutputStream output) throws IOException, SSTException {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(byte[].class, new JsonByteArraySerializer());

    Gson jsonMapper = gsonBuilder.create();
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(output))) {
      for (ListIterator<SSTableReader> iterator = ssTableManager.iteratorInCreationOrder(); iterator.hasNext();) {
        SSTableReader ssTableReader = iterator.next();
        System.out.println("exporting sst file " + ssTableReader.getTableName());
        writer.write(ssTableReader.getTableName());
        writer.write("\r\n");
        for (Iterator<Entry> entryIterator = ssTableReader.entries(); entryIterator.hasNext();) {
          Entry entry = entryIterator.next();
          writer.write(jsonMapper.toJson(entry));
          writer.write("\r\n");
        }
      }
    }
  }
}
