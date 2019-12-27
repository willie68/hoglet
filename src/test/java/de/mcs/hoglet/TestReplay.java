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
    try (HogletDB hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withMemTableMaxKeys(1000).withVlogMaxChunkCount(100))) {

      assertFalse(hogletDB.isReadonly());

      List<byte[]> descs = new ArrayList<>();
      System.out.println("adding keys.");
      byte[] value = new byte[1024];
      for (int i = 0; i < 1500; i++) {
        byte[] key = ids.getByteID();
        descs.add(key);

        new Random().nextBytes(value);

        assertFalse(hogletDB.contains(key));
        assertNull(hogletDB.get(key));

        hogletDB.put(key, value);

        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(value, storedValue));
      }
    }
  }

}
