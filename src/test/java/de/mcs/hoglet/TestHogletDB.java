/**
 * Copyright 2019 w.klaas
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
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author wklaa_000
 *
 */
public class TestHogletDB {

  private static final int MAX_CHUNKS = 10;

  private HogletDB hogletDB;
  private static File dbFolder;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).getFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @BeforeEach
  public void before() throws HogletDBException {
    hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
  }

  @AfterEach
  public void after() throws HogletDBException {
    if (hogletDB != null) {
      hogletDB.close();
    }
  }

  @Test
  public void testEmptyPath() {
    Assertions.assertThrows(HogletDBException.class, () -> {
      try (HogletDB myHogletDB = new HogletDB(Options.defaultOptions())) {
      }
    });
  }

  @Test
  public void testCRUD() throws HogletDBException {
    assertFalse(hogletDB.isReadonly());

    byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[1024];
    new Random().nextBytes(value);

    assertFalse(hogletDB.contains(key));
    assertNull(hogletDB.get(key));

    hogletDB.put(key, value);

    assertTrue(hogletDB.contains(key));

    byte[] storedValue = hogletDB.get(key);
    assertTrue(Arrays.equals(value, storedValue));

    value = new byte[1024];
    new Random().nextBytes(value);
    hogletDB.put(key, value);

    assertTrue(hogletDB.contains(key));

    storedValue = hogletDB.get(key);
    assertTrue(Arrays.equals(value, storedValue));

    byte[] remove = hogletDB.remove(key);
    assertTrue(Arrays.equals(value, remove));

    assertFalse(hogletDB.contains(key));
    assertNull(hogletDB.get(key));
  }

  @Test
  public void testCRUDWithCollection() throws HogletDBException {
    assertFalse(hogletDB.isReadonly());

    String collection = "MCS";
    byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[1024];
    new Random().nextBytes(value);

    assertFalse(hogletDB.contains(collection, key));
    assertNull(hogletDB.get(collection, key));

    hogletDB.put(collection, key, value);

    assertTrue(hogletDB.contains(collection, key));

    byte[] storedValue = hogletDB.get(collection, key);
    assertTrue(Arrays.equals(value, storedValue));

    value = new byte[1024];
    new Random().nextBytes(value);
    hogletDB.put(collection, key, value);

    assertTrue(hogletDB.contains(collection, key));

    storedValue = hogletDB.get(collection, key);
    assertTrue(Arrays.equals(value, storedValue));

    byte[] remove = hogletDB.remove(collection, key);
    assertTrue(Arrays.equals(value, remove));

    assertFalse(hogletDB.contains(collection, key));
    assertNull(hogletDB.get(collection, key));
  }

  @Test
  public void testNullValueInCollection() {
    assertFalse(hogletDB.isReadonly());

    String collection = "MCS" + (char) 0 + "2";
    byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[1024];
    new Random().nextBytes(value);

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      assertFalse(hogletDB.contains(collection, key));
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      byte[] bs = hogletDB.get(collection, key);
      assertNull(bs);
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      hogletDB.put(collection, key, value);
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      hogletDB.remove(collection, key);
    });
  }

  @Test
  public void testChunkUpload() throws IOException {
    assertFalse(hogletDB.isReadonly());

    final String collection = "MCS0003";
    final byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[1024 * 1024];
    new Random().nextBytes(value);

    try (ChunkList chunks = hogletDB.createChunk(collection, key)) {
      for (int i = 0; i < MAX_CHUNKS; i++) {
        Monitor m = MeasureFactory.start("writeChunk");
        try {
          chunks.addChunk(i, value);
        } finally {
          m.stop();
        }
      }
    }

    byte[] newValue = new byte[1024 * 1024];
    int chunkCount = 0;
    Monitor inputM = MeasureFactory.start("createInputStream");
    try (InputStream input = hogletDB.getAsStream(collection, key)) {
      inputM.stop();
      assertNotNull(input);
      while (input.available() > 0) {
        chunkCount++;
        Monitor m = MeasureFactory.start("readChunk");
        try {
          int read = input.read(newValue);
          assertEquals(newValue.length, read);
          assertTrue(Arrays.equals(value, newValue));
        } finally {
          m.stop();
        }
      }
      assertEquals(MAX_CHUNKS, chunkCount);
    }
  }

  @Test
  public void testDirectValues() throws HogletDBException {
    assertFalse(hogletDB.isReadonly());

    byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[64];
    new Random().nextBytes(value);

    assertFalse(hogletDB.contains(key));
    assertNull(hogletDB.get(key));

    hogletDB.put(key, value);

    assertTrue(hogletDB.contains(key));

    byte[] storedValue = hogletDB.get(key);
    assertTrue(Arrays.equals(value, storedValue));

    value = new byte[64];
    new Random().nextBytes(value);
    hogletDB.put(key, value);

    assertTrue(hogletDB.contains(key));

    storedValue = hogletDB.get(key);
    assertTrue(Arrays.equals(value, storedValue));

    byte[] remove = hogletDB.remove(key);
    assertTrue(Arrays.equals(value, remove));

    assertFalse(hogletDB.contains(key));
    assertNull(hogletDB.get(key));
  }

}
