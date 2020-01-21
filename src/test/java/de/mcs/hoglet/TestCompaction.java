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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.SSTException;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.SystemTestFolderHelper;

class TestCompaction {

  private static final int MAX_DOC_COUNT = 30000;
  private static final int DELETE_DOC_COUNT = MAX_DOC_COUNT / 3;
  private static final int MEM_TABLE_MAX_KEYS = 100;
  private static File dbFolder;
  private static List<byte[]> keys;
  private static int number;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    Thread.sleep(1000);
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().getFolder();
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withLvlTableCount(5)
        .withMemTableMaxKeys(MEM_TABLE_MAX_KEYS);
    number = 0;
  }

  @AfterAll
  public static void afterAll() throws IOException {
    SystemTestFolderHelper.outputStatistics(dbFolder, true);
  }

  private List<byte[]> delKeys;
  private List<byte[]> presentKeys;
  private static Options options;

  @Order(1)
  @Test
  public void testCompacting() throws IOException {
    keys = new ArrayList<>();
    delKeys = new ArrayList<>();
    presentKeys = new ArrayList<>();

    byte[] value = new byte[1024];
    new Random(System.currentTimeMillis()).nextBytes(value);

    try (HogletDB hogletDB = new HogletDB(options)) {

      assertFalse(hogletDB.isReadonly());
      int savePercent = 0;

      System.out.println("adding keys.");

      for (int i = 0; i < MAX_DOC_COUNT; i++) {
        byte[] key = ByteArrayUtils.longToBytes(i);
        keys.add(key);
        if (isOdd(key[0])) {
          hogletDB.put(key, key);
        } else {
          hogletDB.put(key, value);
        }
        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        if (isOdd(key[0])) {
          assertTrue(Arrays.equals(key, storedValue));
        } else {
          assertTrue(Arrays.equals(value, storedValue));
        }
        // if ((i % 100) == 0) {
        // System.out.print(".");
        // }
        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
      export(hogletDB);

      System.out.println("testing keys in order.");
      int count = 0;
      for (byte[] key : keys) {

        boolean found = hogletDB.contains(key);
        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        if (isOdd(key[0])) {
          assertTrue(Arrays.equals(key, storedValue));
        } else {
          assertTrue(Arrays.equals(value, storedValue));
        }
        count++;
        // if ((count % 100) == 0) {
        // System.out.print(".");
        // }
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("deleting keys.");
      presentKeys.addAll(keys);
      for (int i = 0; i < DELETE_DOC_COUNT; i++) {
        byte[] bs = presentKeys.remove(0);
        delKeys.add(bs);
      }
      count = 0;
      savePercent = 0;
      for (byte[] key : delKeys) {
        hogletDB.remove(key);
        count++;
        int percent = (count * 100) / delKeys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      export(hogletDB);

      System.out.println("testing deleted keys.");
      count = 0;
      savePercent = 0;
      for (byte[] key : delKeys) {
        boolean test = hogletDB.contains(key);
        assertFalse(hogletDB.contains(key),
            String.format("missing del key number %d, key: %s", count, ByteArrayUtils.bytesAsHexString(key)));
        count++;
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing not deleted keys.");
      count = 0;
      savePercent = 0;
      for (byte[] key : presentKeys) {
        assertTrue(hogletDB.contains(key));
        count++;
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      for (int i = MAX_DOC_COUNT; i < (MAX_DOC_COUNT * 2); i++) {
        byte[] key = ByteArrayUtils.longToBytes(i);
        keys.add(key);
        if (isOdd(key[0])) {
          hogletDB.put(key, key);
        } else {
          hogletDB.put(key, value);
        }
        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        if (isOdd(key[0])) {
          assertTrue(Arrays.equals(key, storedValue));
        } else {
          assertTrue(Arrays.equals(value, storedValue));
        }
        // if ((i % 100) == 0) {
        // System.out.print(".");
        // }
        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
      export(hogletDB);

    }
    System.out.println("ready, waiting for new SST file");

    List<byte[]> myKeys = new ArrayList<>();
    myKeys.addAll(presentKeys);
    System.out.println("shufffle keys");
    Collections.shuffle(myKeys);

    System.out.println("restarting hogletDB");
    try (HogletDB hogletDB = new HogletDB(
        Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      System.out.println("testing keys randomly.");

      int savePercent = 0;
      assertFalse(hogletDB.isReadonly());
      int count = 0;
      for (byte[] key : myKeys) {

        Monitor m = MeasureFactory.start(this, "ContainingContains");
        boolean contains = hogletDB.contains(key);
        m.stop();
        assertTrue(contains, "can't find key count " + count);

        if (isOdd(key[0])) {
          m = MeasureFactory.start(this, "ContainingGetKeyValue");
        } else {
          m = MeasureFactory.start(this, "ContainingGetRandomValue");
        }
        byte[] storedValue = hogletDB.get(key);
        m.stop();
        if (isOdd(key[0])) {
          assertTrue(Arrays.equals(key, storedValue));
        } else {
          assertTrue(Arrays.equals(value, storedValue));
        }
        count++;
        // if ((count % 100) == 0) {
        // System.out.print(".");
        // }
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing deleted keys.");
      count = 0;
      savePercent = 0;
      for (byte[] key : delKeys) {
        boolean test = hogletDB.contains(key);
        assertFalse(hogletDB.contains(key), "del key found, number " + count);
        count++;
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing non imported keys");
      for (int i = 0; i < MAX_DOC_COUNT; i++) {

        byte[] key = ByteArrayUtils.longToBytes(i + MAX_DOC_COUNT);

        Monitor m = MeasureFactory.start(this, "notContainingContains");
        boolean contains = hogletDB.contains(key);
        m.stop();
        assertFalse(contains);

        m = MeasureFactory.start(this, "notContainingGet");
        byte[] storedValue = hogletDB.get(key);
        m.stop();

        assertNull(storedValue);

        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
    }
  }

  @Order(2)
  @Test
  public void testDeletionOfCompactedKeys() throws HogletDBException {
    List<byte[]> delKeys = new ArrayList<>();
    List<byte[]> myKeys = new ArrayList<>();
    myKeys.addAll(keys);
    for (int i = 0; i < DELETE_DOC_COUNT; i++) {
      byte[] bs = myKeys.remove(0);
      delKeys.add(bs);
    }
    System.out.println("restarting hogletDB");
    try (HogletDB hogletDB = new HogletDB(
        Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      assertFalse(hogletDB.isReadonly());

      System.out.println("deleting keys.");
      int count = 0;
      int savePercent = 0;
      for (byte[] key : delKeys) {
        hogletDB.remove(key);
        count++;
        int percent = (count * 100) / delKeys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing deleted keys.");
      count = 0;
      savePercent = 0;
      for (byte[] key : delKeys) {
        boolean test = hogletDB.contains(key);
        assertFalse(hogletDB.contains(key));
        count++;
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing not deleted keys.");
      count = 0;
      savePercent = 0;
      for (byte[] key : myKeys) {
        assertTrue(hogletDB.contains(key));
        count++;
        int percent = (count * 100) / keys.size();
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
    }
  }

  private void export(HogletDB hogletDB) {

    File exportFile = new File(dbFolder, String.format("sst_exp_%d.txt", number));

    try {
      try (OutputStream out = new FileOutputStream(exportFile)) {
        hogletDB.getSSTExporter().export(out);
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
    number++;
  }

  public static boolean isOdd(int i) {
    return (i & 1) != 0;
  }
}
