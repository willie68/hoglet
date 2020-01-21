package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.utils.SSTExporter;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.SystemTestFolderHelper;

class TestDeleteCompact {

  private static final int MAX_DOC_COUNT = 2000;
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

  @Test
  void test() throws HogletDBException {

    keys = new ArrayList<>();
    delKeys = new ArrayList<>();
    presentKeys = new ArrayList<>();

    int docNumber = 0;
    try (HogletDB hogletDB = new HogletDB(options)) {

      assertFalse(hogletDB.isReadonly());
      int savePercent = 0;

      System.out.println("adding keys.");

      for (int i = 0; i < MAX_DOC_COUNT; i++) {
        docNumber++;
        byte[] key = ByteArrayUtils.longToBytes(docNumber);
        keys.add(key);
        hogletDB.put(key, key);
        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(key, storedValue));

        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
      export(hogletDB);

      System.out.println("deleting keys.");
      presentKeys.addAll(keys);
      for (int i = 0; i < DELETE_DOC_COUNT; i++) {
        byte[] bs = presentKeys.remove(0);
        delKeys.add(bs);
      }
      int count = 0;
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

      System.out.println("adding new keys.");
      for (int i = 0; i < MAX_DOC_COUNT; i++) {
        docNumber++;
        byte[] key = ByteArrayUtils.longToBytes(docNumber);
        keys.add(key);
        hogletDB.put(key, key);
        assertTrue(hogletDB.contains(key));

        byte[] storedValue = hogletDB.get(key);
        assertTrue(Arrays.equals(key, storedValue));

        int percent = (i * 100) / MAX_DOC_COUNT;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
      export(hogletDB);

      System.out.printf("ignored by compaction: %d\r\n", hogletDB.getSSTableManager().getIgnored());
    }
  }

  private void export(HogletDB hogletDB) {

    File exportFile = new File(dbFolder, String.format("sst_exp_%d.txt", number));

    try {
      SSTExporter exporter = hogletDB.getSSTExporter();
      try (OutputStream out = new FileOutputStream(exportFile)) {
        exporter.export(out);
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

}
