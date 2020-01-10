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
package de.mcs.hoglet.vlog;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestVLogList {
  private static final boolean DELETE_BEFORE_TEST = true;
  private static final String COLLECTION = "MCS";

  private static QueuedIDGenerator ids;
  private static File dbFolder;
  private static Options options;
  private boolean created = false;

  /**
   * @throws java.lang.Exception
   */
  @BeforeAll
  public static void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withDeleteBeforeTest(true).withRAMDisk(false)
        .getFolder();

    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withVlogMaxChunkCount(10000)
        .withVlogMaxSize(2048L * 1024L * 1024L);
  }

  @AfterAll
  public static void afterAll() {
    System.out.println(MeasureFactory.asString());
  }

  @Order(1)
  @Test
  void testEmpty() {
    try (VLogList vLogList = new VLogList(options)) {
      List<VLog> list = vLogList.getList();
      assertNotNull(list);
      assertEquals(0, list.size());
    }
  }

  @Order(2)
  @Test
  void testReadOnlyEmpty() {
    try (VLogList vLogList = new VLogList(options)) {
      assertFalse(vLogList.isReadonly());
      vLogList.setReadonly(true);
      assertTrue(vLogList.isReadonly());
    }
  }

  @Order(3)
  @Test
  void testReadOnlyVLog() throws IOException {
    try (VLogList vLogList = new VLogList(options)) {
      assertFalse(vLogList.isReadonly());
      try (VLog vLog = vLogList.getNextAvailableVLog()) {
        assertTrue(vLog.isAvailbleForWriting());
      }
      vLogList.setReadonly(true);
      try (VLog vLog = vLogList.getNextAvailableVLog()) {
        assertNull(vLog);
      }
    }
  }

  @Order(4)
  @Test
  void testCreateNewVLogFile() throws IOException {
    if (!created) {
      created = true;
      try (VLogList vLogList = new VLogList(options)) {
        assertFalse(vLogList.isReadonly());
        String vlogName = null;
        try (VLog vLog = vLogList.getNextAvailableVLog()) {
          assertNotNull(vLog);
          vlogName = vLog.getName();
          assertTrue(vLog.isAvailbleForWriting());
          byte[] buffer = new byte[128];
          for (int i = 0; i < buffer.length; i++) {
            if ((i % 10) == 0) {
              buffer[i] = '#';
            } else {
              buffer[i] = (byte) ('0' + (i % 10));
            }
          }
          // new Random().nextBytes(buffer);
          byte[] byteID = ids.getByteID();
          VLogEntryInfo info;

          Monitor m = MeasureFactory.start("write");
          try {
            info = vLog.put(COLLECTION, byteID, 1, buffer, Operation.ADD);
          } finally {
            m.stop();
          }
        }
        try (VLog vLog = vLogList.getNextAvailableVLog()) {
          assertNotNull(vLog);
          assertEquals(vlogName, vLog.getName());
          assertTrue(vLog.isAvailbleForWriting());
        }

        List<VLog> list = vLogList.getList();
        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        assertEquals(vlogName, list.get(0).getName());
        assertTrue(list.get(0).isAvailbleForWriting());

        {
          VLog vLog = vLogList.getVLog(vlogName);
          assertEquals(vlogName, vLog.getName());
          assertTrue(vLog.isAvailbleForWriting());
        }
      }
    }
  }

  @Order(5)
  @Test
  void testReloadVLogFile() throws IOException {
    if (!created) {
      testCreateNewVLogFile();
    }
    try (VLogList vLogList = new VLogList(options)) {
      assertFalse(vLogList.isReadonly());
      String vLogFileName = DatabaseUtils.getVLogFileName(1);
      try (VLog vLog = vLogList.getVLog(vLogFileName)) {
        assertFalse(vLog.isAvailbleForWriting());
      }
    }
  }
}
