package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestCompaction {

  private static final int MAX_DOC_COUNT = 150000;
  private static final int MEM_TABLE_MAX_KEYS = 1000;
  private HogletDB hogletDB;
  private static File dbFolder;
  private static QueuedIDGenerator ids;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    dbFolder = SystemTestFolderHelper.initFolder(true);
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @Test
  public void testCompacting() throws IOException {
    List<byte[]> keys = new ArrayList<>();

    try (HogletDB hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withLvlTableCount(5).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      assertFalse(hogletDB.isReadonly());

      System.out.println("adding keys.");

      for (int i = 0; i < MAX_DOC_COUNT; i++) {
        byte[] key = ids.getByteID();
        keys.add(key);

        hogletDB.put(key, key);
        assertTrue(hogletDB.contains(key));

        // byte[] storedValue = hogletDB.get(key);
        // assertTrue(Arrays.equals(key, storedValue));
        if ((i % 100) == 0) {
          System.out.print(".");
        }
      }

      int count = 0;
      for (byte[] key : keys) {

        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(key, storedValue));

        count++;
        if ((count % 100) == 0) {
          System.out.print(".");
        }
      }

      System.out.println("ready, waiting for new SST file");
    }

    System.out.println("starting hogletDB");
    try (HogletDB hogletDB = new HogletDB(
        Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MEM_TABLE_MAX_KEYS))) {

      assertFalse(hogletDB.isReadonly());
      int count = 0;
      for (byte[] key : keys) {

        assertTrue(hogletDB.contains(key), "can't find key count " + count);

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(key, storedValue));
        count++;
        if ((count % 100) == 0) {
          System.out.print(".");
        }
      }
    }
  }

}
