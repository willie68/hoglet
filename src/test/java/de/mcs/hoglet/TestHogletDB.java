/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: TestHogletDB.java
 * EMail: W.Klaas@gmx.de
 * Created: 05.12.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    dbFolder = SystemTestFolderHelper.initFolder();
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
}
