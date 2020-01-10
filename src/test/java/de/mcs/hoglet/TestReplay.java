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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestReplay {

  private static final int MEM_TABLE_MAX_KEYS = 1000;
  private static final int VLOG_MAX_CHUNK_COUNT = 230;
  private HogletDB hogletDB;
  private static File dbFolder;
  private static QueuedIDGenerator ids;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).getFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @Test
  public void testReplay() throws IOException {
    List<byte[]> keys = new ArrayList<>();
    byte[] value = new byte[128];
    new Random().nextBytes(value);

    try (HogletDB hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withMemTableMaxKeys(MEM_TABLE_MAX_KEYS).withVlogMaxChunkCount(VLOG_MAX_CHUNK_COUNT))) {

      assertFalse(hogletDB.isReadonly());

      System.out.println("adding keys.");

      for (int i = 0; i < 1500; i++) {
        byte[] key = ids.getByteID();
        keys.add(key);

        assertFalse(hogletDB.contains(key));
        assertNull(hogletDB.get(key));

        hogletDB.put(key, value);

        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(value, storedValue));
      }

      for (byte[] key : keys) {

        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(value, storedValue));
      }

      System.out.println("ready, waiting for new SST file");
    }

    System.out.println("restarting hogletDB");
    try (HogletDB hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withMemTableMaxKeys(MEM_TABLE_MAX_KEYS).withVlogMaxChunkCount(VLOG_MAX_CHUNK_COUNT))) {

      assertFalse(hogletDB.isReadonly());
      int count = 0;
      for (byte[] key : keys) {

        assertTrue(hogletDB.contains(key), "can't find key count " + count);
        count++;

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(value, storedValue));
      }
    }
  }

}
