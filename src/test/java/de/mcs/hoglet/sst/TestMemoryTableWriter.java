/**
 * Copyright 2020 w.klaas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mcs.hoglet.sst;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestMemoryTableWriter {

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
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().getFolder();
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
  public static void afterAll() throws IOException {
    SystemTestFolderHelper.outputStatistics(dbFolder, true);
  }

  @Order(1)
  @Test
  void test() throws IOException, SSTException {
    System.out.println("creating key/values");

    keys = new ArrayList<>();
    for (int i = 0; i < MAX_KEYS; i++) {
      byte[] key = ids.getByteID();
      keys.add(key);

      Monitor m = MeasureFactory.start("SortedMemoryTable.add");
      table.add(collection, key, Operation.ADD, key);
      m.stop();
    }

    System.out.println("checking key/values");
    for (byte[] key : keys) {
      Monitor m = MeasureFactory.start("SortedMemoryTable.contains");
      assertTrue(table.containsKey(collection, key));
      m.stop();
    }

    System.out.println("start writing SST");
    int reincarnation = 0;
    SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(level).withNumber(count).withIncarnation(0);
    Monitor mOpen = MeasureFactory.start("MemoryTableWriter.open");
    try (MemoryTableWriter writer = new MemoryTableWriter(options, identity)) {
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
        Monitor m = MeasureFactory.start("MemoryTableWriter.mightContain");
        assertTrue(writer.mightContain(mapKey));
        m.stop();
      }

    }

    File idxFile = DatabaseUtils.getSSTIndexFilePath(dbFolder, identity);
    assertTrue(idxFile.exists());
    System.out.println("checking SST");

    keys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    mOpen = MeasureFactory.start("SSTableReader.open");
    try (SSTableReader reader = SSTableReaderFactory.getReader(options, identity)) {
      mOpen.stop();
      for (byte[] cs : keys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Monitor m = MeasureFactory.start("SSTableReader.mightContain");
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
    int reincarnation = 0;
    SSTIdentity identity = getIdentity(reincarnation);
    try (SSTableReader reader = SSTableReaderFactory.getReader(options, identity)) {
      for (int i = 0; i < 1000; i++) {
        boolean existing = rnd.nextBoolean();
        if (existing) {
          int index = rnd.nextInt(keys.size());
          byte[] cs = keys.get(index);
          MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);

          Monitor m = MeasureFactory.start("SSTableReader.contain");
          assertTrue(reader.mightContain(mapKey));
          m.stop();

          m = MeasureFactory.start("SSTableReader.get");
          Entry entry = reader.get(mapKey);
          m.stop();
          assertNotNull(entry);

          countExisting++;
        } else {

          MapKey mapKey = MapKey.buildPrefixedKey(collection, ids.getByteID());

          Monitor m = MeasureFactory.start("SSTableReader.notContain");
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

      new MemoryTableWriter(options, getIdentity(-1, 0, 0));
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, getIdentity(0, -1, 0));
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, getIdentity(0, 0, -1));
    });
  }

  @Test
  public void testFileCreation() throws IOException, SSTException {
    try (MemoryTableWriter writer = new MemoryTableWriter(options, getIdentity(1, 2, 0))) {
    }
    Assertions.assertThrows(SSTException.class, () -> {
      new MemoryTableWriter(options, getIdentity(1, 2, 0));
    });
  }

  private SSTIdentity getIdentity(int reincarnation) {
    SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(level).withNumber(count)
        .withIncarnation(reincarnation);
    return identity;
  }

  private SSTIdentity getIdentity(int mylevel, int mynumber, int reincarnation) {
    SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(mylevel).withNumber(mynumber)
        .withIncarnation(reincarnation);
    return identity;
  }
}
