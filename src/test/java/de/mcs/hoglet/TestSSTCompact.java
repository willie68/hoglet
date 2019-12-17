/**
 * 
 */
package de.mcs.hoglet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.MemoryTableWriter;
import de.mcs.hoglet.sst.SSTCompacter;
import de.mcs.hoglet.sst.SortedMemoryTable;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author w.klaas
 *
 */
class TestSSTCompact {
  private static final int MAX_KEYS = 10000;

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
    for (int x = 1; x < 11; x++) {

      SortedMemoryTable table = new SortedMemoryTable(options);
      for (int i = 0; i < MAX_KEYS; i++) {
        byte[] key = ids.getByteID();
        keys.add(key);
        Monitor m = MeasureFactory.start("SortedMemoryTable.add");
        table.add(collection, key, key);
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

    SSTCompacter compacter = SSTCompacter.newCompacter(options).withReadingLevel(1).withWritingNumber(1);
    compacter.start();
  }

}
