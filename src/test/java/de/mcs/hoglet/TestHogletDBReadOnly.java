/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: TestHogletDBReadOnly.java
 * EMail: W.Klaas@gmx.de
 * Created: 10.12.2019 wklaa_000
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author wklaa_000
 *
 */
class TestHogletDBReadOnly {

  private static File dbFolder;
  private static QueuedIDGenerator ids;

  @BeforeAll
  public static void beforeAll() throws IOException, InterruptedException {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    dbFolder = SystemTestFolderHelper.initFolder();
  }

  @AfterAll
  public static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @Test
  void testReadOnly() throws HogletDBException {
    HogletDB first = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
    assertFalse(first.isReadonly());

    HogletDB second = new HogletDB(Options.defaultOptions().withPath(dbFolder.getAbsolutePath()));
    assertTrue(second.isReadonly());
    final byte[] key = ids.getByteID();
    first.put("EASY", key, key);

    assertThrows(HogletDBException.class, new Executable() {

      @Override
      public void execute() throws Throwable {
        byte[] key = ids.getByteID();
        second.put("EASY", key, key);
      }
    });

    assertThrows(HogletDBException.class, new Executable() {

      @Override
      public void execute() throws Throwable {
        byte[] key = ids.getByteID();
        second.remove("EASY", key);
      }
    });
  }

}
