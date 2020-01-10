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
package de.mcs.hoglet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.utils.SSTExporter;

class TestSSTExporter {

  public static void main(String[] args) {
    File dbFolder = new File(args[0]);
    File exportFile = new File(dbFolder, "sst_export.txt");

    Options options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath());
    try {
      SSTExporter exporter = new SSTExporter(options);
      try (OutputStream out = new FileOutputStream(exportFile)) {
        exporter.export(out);
      }
    } catch (SSTException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HogletDBException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
