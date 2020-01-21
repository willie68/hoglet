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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
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
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.IDGenerator;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestSSTableReaderRAF {

  private static final int MAX_DOC_TEST = 1000;
  private static final int MAX_KEYS = 64000;
  private static final int MAX_KEYS_BIG = 1000000;
  private static SortedMemoryTable table;
  private static IDGenerator ids;
  private static File dbFolder;
  private static Options options;
  private static List<byte[]> keys;
  private String collection = "Default";
  int level = 1;
  int number = 1;

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
      table.add(collection, key, Operation.ADD, key);
    }

    VLogEntryInfo lastEntry = VLogEntryInfo.newVLogEntryInfo().withEnd(12345);

    System.out.println("checking key/values");
    for (byte[] key : keys) {
      assertTrue(table.containsKey(collection, key));
    }

    System.out.println("start writing SST");
    try (MemoryTableWriter writer = new MemoryTableWriter(options, level, number)) {
      table.forEach(entry -> {
        try {
          writer.write(entry);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      writer.setLastVLogEntry(lastEntry);
      System.out.println("checking bloomfilter of writer.");
    }

    System.out.println("checking SST");

    keys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    Monitor mOpen = MeasureFactory.start(this, "open");
    try (SSTableReaderRAF reader = new SSTableReaderRAF(options, level, number)) {
      mOpen.stop();
      SSTIdentity sstIdentity = reader.getSSTIdentity();
      assertNotNull(sstIdentity);
      assertEquals(level, sstIdentity.getLevel());
      assertEquals(number, sstIdentity.getNumber());

      VLogEntryInfo lastVLogEntry = reader.getLastVLogEntry();
      assertEquals(lastEntry.getEnd(), lastVLogEntry.getEnd());

      assertEquals(DatabaseUtils.getSSTFileName(level, number), reader.getTableName());
      for (byte[] cs : keys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Monitor m = MeasureFactory.start(this, "mightContain");
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
    int savePercent = 0;
    try (SSTableReaderRAF reader = new SSTableReaderRAF(options, level, number)) {
      for (int i = 0; i < MAX_DOC_TEST; i++) {
        boolean existing = rnd.nextBoolean();
        if (existing) {
          int index = rnd.nextInt(keys.size());
          byte[] cs = keys.get(index);
          MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);

          Monitor m = MeasureFactory.start(this, "mightContain");
          assertTrue(reader.mightContain(mapKey));
          m.stop();

          m = MeasureFactory.start(this, "contain");
          assertTrue(reader.contains(mapKey));
          m.stop();

          m = MeasureFactory.start(this, "get");
          Entry entry = reader.get(mapKey);
          m.stop();
          assertNotNull(entry);

          countExisting++;
        } else {

          MapKey mapKey = MapKey.buildPrefixedKey(collection, ids.getByteID());

          Monitor m = MeasureFactory.start(this, "notContain");
          if (reader.mightContain(mapKey)) {
            assertFalse(reader.contains(mapKey));
          }
          m.stop();
        }
        int percent = (i * 100) / MAX_DOC_TEST;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
      }
      assertEquals(0, reader.getMissed());
    }
  }

  @Order(3)
  @Test
  public void testIterator() throws IOException, SSTException {
    System.out.println("SSTableReader: check containing");
    Random rnd = new Random(System.currentTimeMillis());
    List<byte[]> newKeys = new ArrayList<>();
    newKeys.addAll(keys);

    long countExisting = 0;
    try (SSTableReaderRAF reader = new SSTableReaderRAF(options, level, number)) {
      for (Iterator<Entry> iterator = reader.entries(); iterator.hasNext();) {
        Entry next = iterator.next();
        boolean found = false;
        for (Iterator iterator2 = newKeys.iterator(); iterator2.hasNext();) {
          byte[] bs = (byte[]) iterator2.next();
          if (Arrays.equals(bs, next.getKey().getKeyBytes())) {
            found = true;
            iterator2.remove();
            break;
          }
        }
        assertTrue(found);
        countExisting++;
      }
    }
  }

  @Order(4)
  @Test
  public void testNonExistingKeys() throws IOException, SSTException {
    System.out.println("SSTableReader: check non existing keys");

    try (SSTableReaderRAF reader = new SSTableReaderRAF(options, level, number)) {
      for (int i = 0; i < 100; i++) {
        byte[] key = ids.getByteID();
        MapKey mapKey = MapKey.buildPrefixedKey(collection, key);
        assertFalse(reader.contains(mapKey));
        assertNull(reader.get(mapKey));
      }
    }
  }

  @Test
  public void testLevelNumber() {
    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderRAF(options, -1, 0);
    });

    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderRAF(options, 0, -1);
    });
  }

  @Test
  public void testFileCreation() throws IOException, SSTException {
    Assertions.assertThrows(SSTException.class, () -> {
      new SSTableReaderRAF(options, 2, 2);
    });
  }

  @Test
  void testBigFile() throws IOException, SSTException {
    System.out.println("creating key/values");
    int savePercent = 0;

    List<byte[]> myKeys = new ArrayList<>();
    VLogEntryInfo lastEntry = VLogEntryInfo.newVLogEntryInfo().withEnd(12345);
    for (int i = 0; i < MAX_KEYS_BIG; i++) {
      byte[] key = ByteArrayUtils.longToBytes(i);
      myKeys.add(key);
    }

    myKeys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    int bigLevel = 1;
    int bigNumber = 4;
    System.out.println("start writing SST");
    try (MemoryTableWriter writer = new MemoryTableWriter(options, bigLevel, bigNumber)) {
      int count = 0;
      for (byte[] cs : myKeys) {
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);
        Entry entry = new Entry().withKey(mapKey).withOperation(Operation.ADD).withValue(cs);
        writer.write(entry);
        int percent = (count * 100) / MAX_KEYS_BIG;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
        count++;
      }
      writer.setLastVLogEntry(lastEntry);
      System.out.println("checking bloomfilter of writer.");
    }

    System.out.println("checking SST");

    Monitor mOpen = MeasureFactory.start(this, "bigfile.open");
    try (SSTableReaderRAF reader = new SSTableReaderRAF(options, bigLevel, bigNumber)) {
      mOpen.stop();
      SSTIdentity sstIdentity = reader.getSSTIdentity();
      assertNotNull(sstIdentity);
      assertEquals(bigLevel, sstIdentity.getLevel());
      assertEquals(bigNumber, sstIdentity.getNumber());

      VLogEntryInfo lastVLogEntry = reader.getLastVLogEntry();
      assertEquals(lastEntry.getEnd(), lastVLogEntry.getEnd());

      assertEquals(DatabaseUtils.getSSTFileName(bigLevel, bigNumber), reader.getTableName());
      Random rnd = new Random(System.currentTimeMillis());
      int count = 1000;
      while (count > 0) {
        byte[] cs = myKeys.get(rnd.nextInt(myKeys.size()));
        MapKey mapKey = MapKey.buildPrefixedKey(collection, cs);

        Monitor m = MeasureFactory.start(this, "bigfile.mightContain");
        assertTrue(reader.mightContain(mapKey));
        m.stop();

        m = MeasureFactory.start(this, "bigfile.contain");
        assertTrue(reader.contains(mapKey), String.format("on key: %d", ByteArrayUtils.bytesToLong(cs)));
        m.stop();

        int percent = (count * 100) / MAX_KEYS_BIG;
        if (percent != savePercent) {
          System.out.printf("%d %% Percent done.\r\n", percent);
          savePercent = percent;
        }
        count--;
      }
    }
  }
}
