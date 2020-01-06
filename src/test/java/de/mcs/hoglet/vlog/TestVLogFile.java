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
/**
 * 
 */
package de.mcs.hoglet.vlog;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import de.mcs.hoglet.HogletDBException;
import de.mcs.hoglet.Operation;
import de.mcs.hoglet.Options;
import de.mcs.hoglet.utils.DatabaseUtils;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.Files;
import de.mcs.utils.QueuedIDGenerator;
import de.mcs.utils.SystemTestFolderHelper;

/**
 * @author w.klaas
 *
 */
@TestMethodOrder(OrderAnnotation.class)
class TestVLogFile {

  private static final int MAX_DOCS = 1000;
  private static final String COLLECTION = "MCS";
  private static final String COLLECTION_TOO_LONG = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456";
  private static final boolean DELETE_BEFORE_TEST = true;
  private static File dbFolder;
  private static QueuedIDGenerator ids;
  private static Options options;
  private static Map<String, VLogEntryInfo> myMap;
  private static List<String> myIds;
  private static File myVLogFile;

  /**
   * @throws java.lang.Exception
   */
  @BeforeAll
  public static void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    dbFolder = SystemTestFolderHelper.newSystemTestFolderHelper().withRAMDisk(false).withDeleteBeforeTest(true)
        .getFolder();

    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withVlogMaxChunkCount(10000)
        .withVlogMaxSize(2048L * 1024L * 1024L);
    myIds = new ArrayList<>();
    myMap = new HashMap<>();
  }

  @AfterAll
  public static void afterAll() {
    System.out.println(MeasureFactory.asString());
  }

  @Order(1)
  @Test
  void testSingleBin() throws IOException, NoSuchAlgorithmException, InterruptedException {
    System.out.println("test single bin");
    int fileIndex = 11;
    deleteLogFile(fileIndex);

    try (VLogFile vLogFile = new VLogFile(options, fileIndex)) {
      assertTrue(vLogFile.isAvailbleForWriting());
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
        info = vLogFile.put(COLLECTION, byteID, 1, buffer, Operation.ADD);
      } finally {
        m.stop();
      }

      System.out.println(info.toString());

      byteID = ids.getByteID();
      m = MeasureFactory.start("write");
      try {
        info = vLogFile.put(COLLECTION, byteID, 1, buffer, Operation.ADD);
      } finally {
        m.stop();
      }

      System.out.println(info.toString());

      testFileBin(vLogFile, buffer, byteID, info);
    }
  }

  @Order(2)
  @Test
  void test1000Bin() throws IOException, InterruptedException, NoSuchAlgorithmException {
    System.out.println("test 1000 bin");
    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;
    try (VLogFile vLogFile = new VLogFile(options, 12)) {
      myVLogFile = vLogFile.getFile();
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      for (int i = 1; i <= MAX_DOCS; i++) {
        byte[] id = ids.getByteID();
        descs.add(id);
        Monitor m = MeasureFactory.start("write");
        try {
          info = vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
        } finally {
          m.stop();
        }
        if ((i % 100) == 0) {
          System.out.print(".");
        }
        if ((i % 10000) == 0) {
          System.out.println(" " + i);
        }
        infos.put(id, info);
      }
      System.out.println();
      System.out.println("checking");
      int i = 0;
      for (byte[] id : descs) {
        i++;
        if ((i % 100) == 0) {
          System.out.print(".");
        }
        if ((i % 10000) == 0) {
          System.out.println(" " + i);
        }

        testFileBin(vLogFile, buffer, id, infos.get(id));
      }
    }

    descs.forEach(k -> {
      myIds.add(ByteArrayUtils.bytesAsHexString(k));
      VLogEntryInfo vLogEntryInfo = infos.get(k);
      myMap.put(ByteArrayUtils.bytesAsHexString(k), vLogEntryInfo);
    });
    System.out.println();
  }

  @Order(3)
  @Test
  public void testIterator() throws IOException {
    System.out.println("test iterator");
    int count = 0;
    try (VLogFile vLogFile = new VLogFile(options, myVLogFile)) {
      assertTrue(vLogFile.isReadOnly());
      assertFalse(vLogFile.isAvailbleForWriting());

      vLogFile.setReadOnly(true);
      assertTrue(vLogFile.isReadOnly());

      List<VLogEntryDescription> list = new ArrayList<>();
      for (Iterator<VLogEntryDescription> iterator = vLogFile.iterator(); iterator.hasNext();) {
        VLogEntryDescription type = iterator.next();
        // System.out.println(type.toJsonString());
        list.add(type);
        String key = ByteArrayUtils.bytesAsHexString(type.getKey());
        if (myIds.contains(key)) {
          count++;
          myIds.remove(key);
        }
        VLogEntryInfo vLogEntryInfo = myMap.get(key);
        assertNotNull(vLogEntryInfo);
        assertEquals(vLogEntryInfo.end, type.end);
        assertEquals(vLogEntryInfo.hash, ByteArrayUtils.bytesAsHexString(type.hash));
        assertEquals(vLogEntryInfo.start, type.start);
        assertEquals(vLogEntryInfo.startBinary, type.startBinary);
        assertEquals(1, type.chunkNumber);
        assertEquals(vLogFile.getName(), type.containerName);
        assertEquals(COLLECTION, type.collection);
        assertEquals(vLogEntryInfo.getBinarySize(), type.length);
      }
      assertEquals(MAX_DOCS, list.size());
      assertEquals(0, myIds.size());
    }

    System.out.printf("error on id: %d\r\n", ids.getErrorCount());
  }

  private void testFileBin(VLogFile vLogFile, byte[] buffer, byte[] byteId, VLogEntryInfo info) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024 * 2);
    out.reset();
    Monitor m = MeasureFactory.start("readBin");
    try {
      try (InputStream input = vLogFile.get(info.startBinary, info.getBinarySize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }
    assertTrue(Arrays.equals(buffer, out.toByteArray()));

    byte[] value;
    m = MeasureFactory.start("readBinDirect");
    try {
      value = vLogFile.getValue(info.startBinary, info.getBinarySize());
    } finally {
      m.stop();
    }
    assertNotNull(buffer);
    assertTrue(Arrays.equals(buffer, value));

    out.reset();
    m = MeasureFactory.start("readDescr");
    try {
      try (InputStream input = vLogFile.get(info.start, info.getDescriptionSize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    VLogDescriptor descriptor = VLogDescriptor.fromBytes(out.toByteArray());
    assertEquals(info.getBinarySize(), descriptor.length);
    assertEquals(buffer.length, descriptor.length);
    assertEquals(info.hash, descriptor.getHashAsString());
    assertEquals(ByteArrayUtils.bytesAsHexString(byteId), ByteArrayUtils.bytesAsHexString(descriptor.key));
    assertEquals(COLLECTION, new String(descriptor.collectionBytes, StandardCharsets.UTF_8));
    assertEquals(1, descriptor.chunkNumber);

    VLogEntryDescription description;
    m = MeasureFactory.start("readDescrDirect");
    try {
      description = vLogFile.getDescription(info.start, info.getDescriptionSize());
    } finally {
      m.stop();
    }

    assertNotNull(description);
    assertEquals(info.getBinarySize(), descriptor.getLength());
    assertEquals(buffer.length, descriptor.getLength());
    assertEquals(info.hash, descriptor.getHashAsString());
    assertEquals(ByteArrayUtils.bytesAsHexString(byteId), ByteArrayUtils.bytesAsHexString(descriptor.getKey()));
    assertEquals(COLLECTION, new String(descriptor.collectionBytes, StandardCharsets.UTF_8));
    assertEquals(1, descriptor.getChunkNumber());
  }

  @Order(4)
  @Test
  public void testVLogToBig() throws IOException, InterruptedException {
    System.out.println("testing max. vlog file length");
    int fileIndex = 13;
    deleteLogFile(fileIndex);

    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;

    Options myOptions = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withVlogMaxChunkCount(10000)
        .withVlogMaxSize(128L * 1024L * 1024L);

    try (VLogFile vLogFile = new VLogFile(myOptions, fileIndex)) {
      myVLogFile = vLogFile.getFile();
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      for (int i = 1; i <= 130; i++) {
        byte[] id = ids.getByteID();
        Monitor m = MeasureFactory.start("write");
        try {
          if (i >= 129) {
            System.out.println("creating " + i + " blob");
            Assertions.assertThrows(HogletDBException.class, () -> {
              vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
            });
          } else {
            descs.add(id);
            info = vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
          }
        } finally {
          m.stop();
        }
        if ((i % 100) == 0) {
          System.out.print(".");
        }
        infos.put(id, info);
      }
      for (byte[] id : descs) {
        testFileBin(vLogFile, buffer, id, infos.get(id));
      }
    }
  }

  @Order(5)
  @Test
  public void testMaxChunksInVlog() throws IOException, InterruptedException {
    System.out.println("testing max. chunks in vlog file");
    int fileIndex = 14;
    deleteLogFile(fileIndex);

    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;

    Options myOptions = Options.defaultOptions().withPath(dbFolder.getAbsolutePath()).withVlogMaxChunkCount(999)
        .withVlogMaxSize(1024L * 1024L * 1024L);

    try (VLogFile vLogFile = new VLogFile(myOptions, fileIndex)) {
      myVLogFile = vLogFile.getFile();
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      for (int i = 1; i <= 1000; i++) {
        byte[] id = ids.getByteID();
        Monitor m = MeasureFactory.start("write");
        try {
          if (i == 1000) {
            Assertions.assertThrows(HogletDBException.class, () -> {
              vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
            });
          } else {
            descs.add(id);
            info = vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
          }
        } finally {
          m.stop();
        }
        if ((i % 100) == 0) {
          System.out.print(".");
        }
        infos.put(id, info);
      }
      for (byte[] id : descs) {

        // System.out.println(infos.get(id).toString());

        testFileBin(vLogFile, buffer, id, infos.get(id));
      }
    }
    System.out.println();
  }

  @Order(6)
  @Test
  public void testDifferentVlogPath() throws IOException, InterruptedException {
    System.out.println("testing different vlog file path");
    int fileIndex = 15;
    deleteLogFile(fileIndex);

    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;

    File vlogFolder = new File(dbFolder, "vlog");
    if (!vlogFolder.exists()) {
      vlogFolder.mkdirs();
    }

    Options myOptions = Options.defaultOptions().withPath(dbFolder.getAbsolutePath())
        .withVlogPath(vlogFolder.getAbsolutePath());

    File vlogFile = new File(vlogFolder, DatabaseUtils.getVLogFileName(fileIndex));
    assertFalse(vlogFile.exists());

    try (VLogFile vLogFile = new VLogFile(myOptions, fileIndex)) {
      myVLogFile = vLogFile.getFile();
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      for (int i = 1; i <= 100; i++) {
        byte[] id = ids.getByteID();
        Monitor m = MeasureFactory.start("write");
        try {
          descs.add(id);
          info = vLogFile.put(COLLECTION, id, 1, buffer, Operation.ADD);
        } finally {
          m.stop();
        }
        infos.put(id, info);
      }
      for (byte[] id : descs) {
        testFileBin(vLogFile, buffer, id, infos.get(id));
      }
    }
    assertTrue(vlogFile.exists());
  }

  @Test
  public void testCollectionAndKeyLimits() throws IOException, InterruptedException {
    System.out.println("test testCollectionAndKeyLimits");
    int fileIndex = 15;
    deleteLogFile(fileIndex);
    try (VLogFile vLogFile = new VLogFile(options, fileIndex)) {
      assertTrue(vLogFile.isAvailbleForWriting());
      byte[] buffer = new byte[257];
      for (int i = 0; i < buffer.length; i++) {
        if ((i % 10) == 0) {
          buffer[i] = '#';
        } else {
          buffer[i] = (byte) ('0' + (i % 10));
        }
      }
      // new Random().nextBytes(buffer);
      byte[] byteID = ids.getByteID();
      Assertions.assertThrows(HogletDBException.class, () -> {
        vLogFile.put(COLLECTION_TOO_LONG, byteID, 1, buffer, Operation.ADD);
      });

      Assertions.assertThrows(HogletDBException.class, () -> {
        vLogFile.put(COLLECTION, buffer, 1, buffer, Operation.ADD);
      });
    }

  }

  private void deleteLogFile(int i) throws IOException, InterruptedException {
    File vlogFile = VLogFile.getFilePathName(dbFolder, i);
    if (vlogFile.exists()) {
      Files.remove(vlogFile, true);
      Thread.sleep(100);
    }
  }
}
