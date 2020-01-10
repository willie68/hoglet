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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ListIterator;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.SSTableManager;
import de.mcs.hoglet.sst.SSTableReader;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author wklaa_000
 *
 */
public class TestHogletDBPerformance {

  private static final int MAX_DOC_COUNT = 1000000;

  private HogletDB hogletDB;
  private static File dbFolder;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withRAMDisk(true).withDeleteBeforeTest(true)
        .getFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @BeforeEach
  public void before() throws HogletDBException {
    hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withSstIndexPreload(true));
  }

  @AfterEach
  public void after() throws HogletDBException {
    SSTableManager ssTableManager = hogletDB.getSSTableManager();
    for (ListIterator<SSTableReader> iterator = ssTableManager.iteratorInCreationOrder(); iterator.hasNext();) {
      SSTableReader ssTableReader = iterator.next();
      System.out.printf("Table: %s, missed: %d\r\n", ssTableReader.getTableName(), ssTableReader.getMissed());
    }
    if (hogletDB != null) {
      hogletDB.close();
    }
  }

  @Test
  public void testperformance() throws HogletDBException {
    int savePercent = 0;

    assertFalse(hogletDB.isReadonly());

    byte[] value = new byte[127];
    new Random().nextBytes(value);

    System.out.println("writing");
    Monitor m = MeasureFactory.start(this, "overallWrite");
    for (int i = 0; i < MAX_DOC_COUNT; i++) {

      byte[] key = ByteArrayUtils.longToBytes(i);
      Monitor mPut = MeasureFactory.start(this, "put");
      hogletDB.put(key, value);
      mPut.stop();

      int percent = (i * 100) / MAX_DOC_COUNT;
      if (percent != savePercent) {
        System.out.printf("%d %% Percent done.\r\n", percent);
        savePercent = percent;
      }
    }
    m.stop();

    System.out.println("reading");
    m = MeasureFactory.start(this, "overallRead");
    for (int i = 0; i < MAX_DOC_COUNT; i++) {

      byte[] key = ByteArrayUtils.longToBytes(i);
      // assertTrue(hogletDB.contains(key));
      Monitor mGet = MeasureFactory.start(this, "get");
      byte[] storedValue = hogletDB.get(key);
      mGet.stop();
      assertTrue(Arrays.equals(value, storedValue));

      int percent = (i * 100) / MAX_DOC_COUNT;
      if (percent != savePercent) {
        System.out.printf("%d %% Percent done.\r\n", percent);
        savePercent = percent;
      }
    }
    m.stop();
  }

}
