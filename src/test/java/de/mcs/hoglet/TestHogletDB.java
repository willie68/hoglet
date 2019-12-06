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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.utils.Files;
import de.mcs.utils.SystemHelper;

/**
 * @author wklaa_000
 *
 */
public class TestHogletDB {

  private static final String DB_FOLDER_PATH = "e:/temp/hogletdb/";
  private static final String EASY_DB_FOLDER_PATH = "h:/temp/hogletdb/";

  private HogletDB hogletDB;
  private static File dbFolder;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    deleteFolder();
  }

  private static void deleteFolder() throws IOException, InterruptedException {
    dbFolder = new File(DB_FOLDER_PATH);
    if ("CBP1ZF2".equals(SystemHelper.getComputerName())) {
      dbFolder = new File(EASY_DB_FOLDER_PATH);
    }
    if (dbFolder.exists()) {
      Files.remove(dbFolder, true);
      Thread.sleep(100);
    }
    dbFolder.mkdirs();
  }

  @BeforeEach
  public void before() throws HogletDBException {
    hogletDB = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
  }

  @AfterEach
  public void after() {
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
    String collection = "MCS" + (char) 0 + "2";
    byte[] key = UUID.randomUUID().toString().getBytes();

    byte[] value = new byte[1024];
    new Random().nextBytes(value);

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      assertFalse(hogletDB.contains(collection, key));
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      assertNull(hogletDB.get(collection, key));
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      hogletDB.put(collection, key, value);
    });

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      hogletDB.remove(collection, key);
    });
  }
}
