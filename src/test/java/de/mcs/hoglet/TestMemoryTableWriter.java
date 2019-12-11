package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.memorytable.MemoryTableWriter;
import de.mcs.hoglet.memorytable.SSTException;
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
  private File dbFolder;
  private Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.initFolder(true);
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath());
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
  }

  @Test
  void test() throws IOException, SSTException {
    System.out.println("creating key/values");
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < MAX_KEYS; i++) {
      byte[] key = ids.getByteID();
      keys.add(key);
      Monitor m = MeasureFactory.start("SortedMemoryTable.add");
      table.add(collection, key, key);
      m.stop();
    }

    System.out.println("checking key/values");
    for (byte[] key : keys) {
      Monitor m = MeasureFactory.start("SortedMemoryTable.contains");
      assertTrue(table.containsKey(collection, key));
      m.stop();
    }

    System.out.println("start writing SST");

    int level = 1;
    int count = 1;
    try (MemoryTableWriter writer = new MemoryTableWriter(options, level, count)) {

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

    System.out.println("checking SST");

  }

}
