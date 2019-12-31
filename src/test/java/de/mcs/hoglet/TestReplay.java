package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    dbFolder = SystemTestFolderHelper.initFolder(true);
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
        // assertTrue(Arrays.equals(value, storedValue));
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
        // assertTrue(Arrays.equals(value, storedValue));
      }
    }
  }

}
