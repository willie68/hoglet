package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestCompaction {

  private static final int MAX_DOC_COUNT = 30000;
  private static final int MEM_TABLE_MAX_KEYS = 100;
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
  public void testCompacting() throws IOException {
    List<byte[]> keys = new ArrayList<>();

    byte[] value = new byte[1024];
    new Random(System.currentTimeMillis()).nextBytes(value);

    try (HogletDB hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withLvlTableCount(5).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      assertFalse(hogletDB.isReadonly());
      int savePercent = 0;

      System.out.println("adding keys.");

      for (int i = 0; i < MAX_DOC_COUNT; i++) {
        byte[] key = ids.getByteID();
        keys.add(key);
        if (isOdd(key[0])) {
          hogletDB.put(key, key);
        } else {
          hogletDB.put(key, value);
        }
        assertTrue(hogletDB.contains(key));

        // byte[] storedValue = hogletDB.get(key);
        // assertTrue(Arrays.equals(key, storedValue));
        // if ((i % 100) == 0) {
        // System.out.print(".");
        // }
        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }

      System.out.println("testing keys in order.");
      int count = 0;
      for (byte[] key : keys) {

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

      System.out.println("ready, waiting for new SST file");
    }

    System.out.println("shufffle keys");
    Collections.shuffle(keys);

    System.out.println("restarting hogletDB");
    try (HogletDB hogletDB = new HogletDB(
        Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      System.out.println("testing keys randomly.");

      int savePercent = 0;
      assertFalse(hogletDB.isReadonly());
      int count = 0;
      for (byte[] key : keys) {

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

      System.out.println("testing non imported keys");
      for (int i = 0; i < MAX_DOC_COUNT; i++) {

        byte[] key = ids.getByteID();

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

  public static boolean isOdd(int i) {
    return (i & 1) != 0;
  }
}
