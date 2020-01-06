package de.mcs.hoglet.sst;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestSSTableReaderMMF {

  private static final int MAX_KEYS = 64000;
  private static SortedMemoryTable table;
  private static IDGenerator ids;
  private static File dbFolder;
  private static Options options;
  private static List<byte[]> keys;
  private String collection = "Default";
  int level = 1;
  int count = 1;

  /**
   * @throws java.lang.Exception
   */
  @BeforeAll
  public static void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).getFolder();
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(100000);
    table = new SortedMemoryTable(options);
    ids = new QueuedIDGenerator(10000);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterEach
  void tearDown() throws Exception {
  }

  @AfterAll
  static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
    System.out.println("finnished");
  }

  @Order(1)
  @Test
  void test() throws IOException, SSTException {
    System.out.println("creating key/values");

    keys = new ArrayList<>();
    for (int i = 0; i < MAX_KEYS; i++) {
      byte[] key = ids.getByteID();
      keys.add(key);
      table.add(collection, key, Operation.ADD, key);
    }

    System.out.println("checking key/values");
    for (byte[] key : keys) {
      assertTrue(table.containsKey(collection, key));
    }

    System.out.println("start writing SST");
    try (MemoryTableWriter writer = new MemoryTableWriter(options, level, count)) {
      table.forEach(entry -> {
        try {
          writer.write(entry);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      System.out.println("checking bloomfilter of writer.");
    }

    System.out.println("checking SST");

    keys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    Monitor mOpen = MeasureFactory.start("SSTableReaderMMF.open");
    try (SSTableReaderMMF reader = new SSTableReaderMMF(options, level, count)) {
      mOpen.stop();
      for (byte[] cs : keys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Monitor m = MeasureFactory.start("SSTableReaderMMF.mightContain");
        assertTrue(reader.mightContain(mapKey));
        m.stop();
      }
    }
  }

  @Order(2)
  @Test
  void testAccess() throws IOException, SSTException {
    System.out.println("SSTableReader: check containing");
    Random rnd = new Random(System.currentTimeMillis());
    long countExisting = 0;
    try (SSTableReaderMMF reader = new SSTableReaderMMF(options, level, count)) {
      for (int i = 0; i < 1000; i++) {
        boolean existing = rnd.nextBoolean();
        if (existing) {
          int index = rnd.nextInt(keys.size());
          byte[] cs = keys.get(index);
          MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);

          Monitor m = MeasureFactory.start("SSTableReaderMMF.contain");
          assertTrue(reader.mightContain(mapKey));
          m.stop();

          m = MeasureFactory.start("SSTableReaderMMF.get");
          Entry entry = reader.get(mapKey);
          m.stop();
          assertNotNull(entry);

          countExisting++;
        } else {

          MapKey mapKey = MapKey.buildPrefixedKey(collection, ids.getByteID());

          Monitor m = MeasureFactory.start("SSTableReaderMMF.notContain");
          if (reader.mightContain(mapKey)) {
            reader.contains(mapKey);
          }
          m.stop();
        }
      }
    }
  }

  @Test
  public void testLevelNumber() {
    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderMMF(options, -1, 0);
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderMMF(options, 0, -1);
    });
  }

  @Test
  public void testFileCreation() throws IOException, SSTException {
    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderMMF(options, 2, 2);
    });
  }
}
