package de.mcs.hoglet;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.sst.MapKey;
import de.mcs.hoglet.sst.MemoryTableWriter;
import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.sst.SSTableReader;
import de.mcs.hoglet.sst.SortedMemoryTable;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestMemoryTableWriter {

  private static final int MAX_KEYS = 64000;
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
    Monitor mOpen = MeasureFactory.start("MemoryTableWriter.open");
    try (MemoryTableWriter writer = new MemoryTableWriter(options, level, count)) {
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
      System.out.println("checking bloomfilter of writer.");
      for (byte[] cs : keys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Monitor m = MeasureFactory.start("SSTTableReader.mightContain");
        assertTrue(writer.mightContain(mapKey));
        m.stop();
      }
    }

    System.out.println("checking SST");

    keys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    mOpen = MeasureFactory.start("SSTTableReader.open");
    try (SSTableReader reader = new SSTableReader(options, level, count)) {
      mOpen.stop();
      for (byte[] cs : keys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Monitor m = MeasureFactory.start("SSTTableReader.mightContain");
        assertTrue(reader.mightContain(mapKey));
        m.stop();

        m = MeasureFactory.start("SSTTableReader.read");
        Entry<MapKey, byte[]> entry = reader.read();
        m.stop();
        assertArrayEquals(mapKey.getKey(), entry.getKey().getKey());
        assertArrayEquals(cs, entry.getValue());
      }

    }
  }

  @Test
  public void testLevelNumber() {
    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, -1, 0);
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, 0, -1);
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReader(options, -1, 0);
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReader(options, 0, -1);
    });
  }

  @Test
  public void testFileCreation() throws IOException, SSTException {
    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReader(options, 2, 2);
    });

    try (MemoryTableWriter writer = new MemoryTableWriter(options, 1, 2)) {
    }
    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, 1, 2);
    });
  }
}
