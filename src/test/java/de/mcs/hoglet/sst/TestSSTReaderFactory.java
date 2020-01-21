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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import de.mcs.hoglet.Options;
import de.mcs.utils.SystemTestFolderHelper;

class TestSSTReaderFactory {

  private static File dbFolder;
  private static Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeAll
  static void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().getFolder();
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterEach
  void tearDown() throws Exception {
  }

  @AfterAll
  public static void afterAll() throws IOException {
    SystemTestFolderHelper.outputStatistics(dbFolder, true);
  }

  @Test
  void test() throws IOException, SSTException {
    assertThrows(SSTException.class, new Executable() {
      @Override
      public void execute() throws Throwable {
        try (SSTableReader reader = SSTableReaderFactory.getReader(options, 0, 0)) {
        }
      }
    });
  }

}
