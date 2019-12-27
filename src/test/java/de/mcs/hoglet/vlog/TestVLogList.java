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
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

class TestVLogList {
  private static final boolean DELETE_BEFORE_TEST = true;
  private static final String COLLECTION = "MCS";

  private static QueuedIDGenerator ids;
  private static File filePath;
  private static Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeAll
  public static void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    filePath = SystemTestFolderHelper.initFolder(DELETE_BEFORE_TEST);

    options = Options.defaultOptions().withPath(filePath.getAbsolutePath()).withVlogMaxChunkCount(10000)
        .withVlogMaxSize(2048L * 1024L * 1024L);
  }

  @AfterAll
  public static void afterAll() {
    System.out.println(MeasureFactory.asString());
  }

  @Test
  @Order(1)
  void testEmpty() {
    try (VLogList vLogList = new VLogList(options)) {
      List<VLog> list = vLogList.getList();
      assertNotNull(list);
      assertEquals(0, list.size());
    }
  }

  @Test
  @Order(1)
  void testReadOnlyEmpty() {
    try (VLogList vLogList = new VLogList(options)) {
      assertFalse(vLogList.isReadonly());
      vLogList.setReadonly(true);
      assertTrue(vLogList.isReadonly());
    }
  }

  @Test
  @Order(2)
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

  @Test
  @Order(3)
  void testCreateNewVLogFile() throws IOException {
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
