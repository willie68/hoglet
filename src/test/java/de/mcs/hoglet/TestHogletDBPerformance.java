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
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author wklaa_000
 *
 */
public class TestHogletDBPerformance {

  private static final int MAX_CHUNKS = 10;
  private static final int MAX_DOC_COUNT = 100000;

  private HogletDB hogletDB;
  private static File dbFolder;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).getFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @BeforeEach
  public void before() throws HogletDBException {
    hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
  }

  @AfterEach
  public void after() throws HogletDBException {
    if (hogletDB != null) {
      hogletDB.close();
    }
  }

  @Test
  public void testCRUD() throws HogletDBException {
    assertFalse(hogletDB.isReadonly());

    byte[] value = new byte[128];
    new Random().nextBytes(value);

    System.out.println("writing");
    Monitor m = MeasureFactory.start(this, "overallWrite");
    for (int i = 0; i < MAX_DOC_COUNT; i++) {

      byte[] key = ByteArrayUtils.longToBytes(i);
      hogletDB.put(key, value);
    }
    m.stop();

    System.out.println("reading");
    m = MeasureFactory.start(this, "overallRead");
    for (int i = 0; i < MAX_DOC_COUNT; i++) {

      byte[] key = ByteArrayUtils.longToBytes(i);
      // assertTrue(hogletDB.contains(key));

      byte[] storedValue = hogletDB.get(key);
      assertTrue(Arrays.equals(value, storedValue));
    }
    m.stop();
  }

}
