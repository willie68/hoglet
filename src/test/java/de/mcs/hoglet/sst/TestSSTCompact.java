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
/**
 * 
 */
package de.mcs.hoglet.sst;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import de.mcs.hoglet.HogletDB;
import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.hoglet.utils.SSTUtils;
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
  private static final int MAX_DELTE_KEYS_ADD = 1000;
  private static final int MAX_KEYS = 10000;
  private static final int MAX_ROUNDS = 10;

  private IDGenerator ids;
  private File dbFolder;
  private Options options;
  private boolean useBaseFolder = false;
  private HogletDB hogletDB;

  @BeforeAll
  static void beforeAll() {
    MockitoAnnotations.initMocks(TestSSTCompact.class);
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true)
        .withUseOnlyBaseFolder(useBaseFolder).getFolder();
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withMemTableMaxKeys(MAX_KEYS);
    ids = new QueuedIDGenerator(10000);
    hogletDB = mock(HogletDB.class);
    when(hogletDB.containsKeyUptoSST(anyString(), any(), any())).thenReturn(false);
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
  void testNormalComaption() throws Exception {
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
    for (int x = 0; x < MAX_ROUNDS; x++) {

      SortedMemoryTable table = new SortedMemoryTable(options);
      for (int i = 0; i < MAX_KEYS; i++) {
        int value = rnd.nextInt(keys.size());
        byte[] key = keys.remove(value);
        Monitor m = MeasureFactory.start("SortedMemoryTable.add");
        if (isOdd(key[key.length - 1])) {
          table.add(collection, key, Operation.ADD, key);
        } else {
          table.add(collection, key, Operation.DELETE, key);
        }
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
    SSTCompacter compacter = SSTCompacter.newCompacter(options).withReadingLevel(1).withWritingNumber(1)
        .withHogletDB(hogletDB);
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
          if (isOdd(bs[bs.length - 1])) {
            assertTrue(Operation.ADD.equals(entry.getOperation()));
          } else {
            assertTrue(Operation.DELETE.equals(entry.getOperation()));
          }
        }
      }
    }
  }

  @Test
  void testDeleteComaption() throws Exception {
    String collection = "Default";
    List<byte[]> keys = new ArrayList<>();
    for (long x = 0; x < 2 * MAX_DELTE_KEYS_ADD; x++) {
      long nr = x;
      byte[] key = ByteArrayUtils.longToBytes(nr);
      keys.add(key);
    }
    options.setMemTableMaxKeys(MAX_DELTE_KEYS_ADD);

    SortedMemoryTable table = new SortedMemoryTable(options);
    for (int i = 0; i < MAX_DELTE_KEYS_ADD; i++) {
      byte[] key = keys.get(i);
      table.add(collection, key, Operation.ADD, key);
    }
    SSTUtils.writeMemoryTable(options, 0, table);

    table = new SortedMemoryTable(options);
    for (int i = 0; i < MAX_DELTE_KEYS_ADD; i++) {
      byte[] key = keys.get(i + MAX_DELTE_KEYS_ADD);
      table.add(collection, key, Operation.ADD, key);
    }
    SSTUtils.writeMemoryTable(options, 1, table);

    System.out.println("start compacting");

    SSTCompacter compacter = SSTCompacter.newCompacter(options).withReadingLevel(0).withWritingNumber(1)
        .withHogletDB(hogletDB);
    compacter.start();

    DatabaseUtils dbUtils = DatabaseUtils.newDatabaseUtils(options);

    assertEquals(2, dbUtils.getSSTFileCount(0));
    assertEquals(1, dbUtils.getSSTFileCount(1));

  }

  public static boolean isOdd(int i) {
    return (i & 1) != 0;
  }

}
