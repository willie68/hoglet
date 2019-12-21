/**
 * 
 */
package de.mcs.hoglet.sst;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author w.klaas
 *
 */
class TestSSTCompact {
  private static final int MAX_KEYS = 10000;
  private static final int MAX_ROUNDS = 10;

  private IDGenerator ids;
  private File dbFolder;
  private Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.initFolder(true);
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MAX_KEYS);
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
  }

  @Test
  void test() throws Exception {
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (long x = 0; x < MAX_KEYS * MAX_ROUNDS; x++) {
      long nr = x;
      byte[] key = ByteArrayUtils.longToBytes(nr);
      keys.add(key);

    }
    List<byte[]> saveKeys = new ArrayList<>();
    keys.forEach(key -> saveKeys.add(key));
    Random rnd = new Random(System.currentTimeMillis());
    for (int x = 1; x <= MAX_ROUNDS; x++) {

      SortedMemoryTable table = new SortedMemoryTable(options);
      for (int i = 0; i < MAX_KEYS; i++) {
        int value = rnd.nextInt(keys.size());
        byte[] key = keys.remove(value);
        Monitor m = MeasureFactory.start("SortedMemoryTable.add");
        table.add(collection, key, Operation.ADD, key);
        m.stop();
      }

      Monitor mOpen = MeasureFactory.start("MemoryTableWriter.open");
      try (MemoryTableWriter writer = new MemoryTableWriter(options, 1, x)) {
        mOpen.stop();

        table.forEach(entry -> {
          Monitor m = MeasureFactory.start("MemoryTableWriter.write");
          try {
            writer.write(entry);
          } catch (IOException e) {
            e.printStackTrace();
          }
          m.stop();
        });
      }
    }

    System.out.println("start compacting");
    SSTCompacter compacter = SSTCompacter.newCompacter(options).withReadingLevel(1).withWritingNumber(1);
    compacter.start();

    DatabaseUtils dbUtils = DatabaseUtils.newDatabaseUtils(options);
    assertEquals(10, dbUtils.getSSTFileCount(1));
    assertEquals(1, dbUtils.getSSTFileCount(2));

    System.out.println("start reading");
    try (SSTableReader reader = new SSTableReaderMMF(options, 2, 1)) {
      for (byte[] bs : saveKeys) {
        MapKey key = MapKey.buildPrefixedKey(collection, bs);
        assertTrue(reader.mightContain(key));
        if (rnd.nextInt(1000) == 1) {
          Monitor m = MeasureFactory.start("SSTableReader.read");
          Entry entry = reader.get(key);
          m.stop();
          assertNotNull(entry);
          assertTrue(key.equals(entry.getKey()));
          assertEquals(collection, entry.getKey().getCollection());
          assertTrue(Arrays.equals(key.getKeyBytes(), entry.getValue()));
        }
      }
    }
  }

}
