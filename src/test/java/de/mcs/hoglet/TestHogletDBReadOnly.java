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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author wklaa_000
 *
 */
class TestHogletDBReadOnly {

  private static File dbFolder;
  private static QueuedIDGenerator ids;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).getFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @Test
  void testReadOnly() throws HogletDBException {
    HogletDB first = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
    assertFalse(first.isReadonly());

    HogletDB second = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
    assertTrue(second.isReadonly());
    final byte[] key = ids.getByteID();
    first.put("EASY", key, key);

    assertThrows(HogletDBException.class, new Executable() {

      @Override
      public void execute() throws Throwable {
        byte[] key = ids.getByteID();
        second.put("EASY", key, key);
      }
    });

    assertThrows(HogletDBException.class, new Executable() {

      @Override
      public void execute() throws Throwable {
        byte[] key = ids.getByteID();
        second.remove("EASY", key);
      }
    });
  }

}
