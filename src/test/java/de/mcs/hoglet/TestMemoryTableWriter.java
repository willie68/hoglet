package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.memorytable.MemoryTableWriter;
import de.mcs.hoglet.memorytable.SortedMemoryTable;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestMemoryTableWriter {

  private static final int MAX_KEYS = 500000;
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
  void test() throws IOException {
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
    }

    int level = 1;
    int count = 1;
    try (MemoryTableWriter writer = new MemoryTableWriter(Options.defaultOptions(), level, count)) {

      table.forEach(entry -> {
        Monitor m = MeasureFactory.start("MemoryTableWriter.write");
        writer.write(entry);
        m.stop();
      });
    }
  }

}
