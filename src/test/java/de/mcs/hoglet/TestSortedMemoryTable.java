/**
 * 
 */
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.SortedMemoryTable;
import de.mcs.jmeasurement.DefaultMonitor;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.MeasurePoint;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author w.klaas
 *
 */
class TestSortedMemoryTable {

  private static final int MAX_KEYS = 50000;
  private static final int MAX_TEST_KEYS = 100000;
  private SortedMemoryTable table;
  private IDGenerator ids;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    table = new SortedMemoryTable(Options.defaultOptions());
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
  void testBloomfilter() {
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < MAX_KEYS; i++) {
      byte[] key = ids.getByteID();
      keys.add(key);
      Monitor m = MeasureFactory.start("SortedMemoryTable.add");
      table.add(collection, key, key);
      m.stop();
    }

    for (byte[] key : keys) {
      Monitor m = MeasureFactory.start("SortedMemoryTable.contains");
      assertTrue(table.containsKey(collection, key));
      m.stop();

      m = MeasureFactory.start("SortedMemoryTable.get");
      byte[] bs = table.get(collection, key);
      m.stop();
      assertTrue(Arrays.equals(key, bs));
    }

    for (int i = 0; i < MAX_TEST_KEYS; i++) {
      byte[] key = ids.getByteID();

      Monitor m = MeasureFactory.start("SortedMemoryTable.contains");
      assertFalse(table.containsKey(collection, key));
      m.stop();
    }

    assertEquals(MAX_KEYS, table.size());

    MeasurePoint p = MeasureFactory.getMeasurePoint("SortedMemoryTable.Bloomfilter.missed");
    Monitor m = new DefaultMonitor();
    m.increase(table.getMissed());
    p.processMonitor(m);
    float percent = (((float) table.getMissed()) / MAX_TEST_KEYS);

    assertTrue(percent < 0.01, String.format("returning %f", percent));
  }

  @Test
  void testMaxKeys() {
    SortedMemoryTable myTable = new SortedMemoryTable(Options.defaultOptions().withMemTableMaxKeys(1000));
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      byte[] key = ids.getByteID();
      assertTrue(myTable.isAvailbleForWriting());
      Monitor m = MeasureFactory.start("SortedMemoryTable.add");
      myTable.add(collection, key, key);
      m.stop();
    }

    byte[] key = ids.getByteID();
    assertFalse(myTable.isAvailbleForWriting());
    myTable.add(collection, key, key);
    // Assertions.assertThrows(HogletDBException.class, () -> {
    // });
  }

  @Test
  void testMaxSize() {
    int maxTableSize = 1024 * 1024 * 10;
    SortedMemoryTable myTable = new SortedMemoryTable(
        Options.defaultOptions().withMemTableMaxSize(maxTableSize).withMemTableMaxKeys(1000000));
    int countBytes = 0;
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < 1000000; i++) {
      byte[] bs = ids.getByteID();
      // System.out.printf("%07d: %d %d\n", i, maxTableSize, countBytes);
      if (countBytes <= maxTableSize) {
        assertTrue(myTable.isAvailbleForWriting(), String.format("error on %d", i));
        myTable.add(collection, bs, bs);
      } else {
        assertFalse(myTable.isAvailbleForWriting());
        break;
      }
      // countBytes += prefixedKey.getKey().length + value.length + 40
      countBytes += collection.length() + 1 + bs.length + 40 + bs.length;
    }

    byte[] key = ids.getByteID();
    assertFalse(myTable.isAvailbleForWriting());
    myTable.add(collection, key, key);
    // Assertions.assertThrows(HogletDBException.class, () -> {
    // });
  }
}
