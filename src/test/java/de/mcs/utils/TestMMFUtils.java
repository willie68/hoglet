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
package de.mcs.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.junit.jupiter.api.Test;

class TestMMFUtils {

  @Test
  void testIsSystemCapableOfUnmapMMF() throws FileNotFoundException, IOException {
    assertTrue(MMFUtils.isSystemCapableOfUnmapMMF());
  }

  @Test
  void testUnmapMMF() throws FileNotFoundException, IOException {
    File file = File.createTempFile("test", ".mmf");
    file.deleteOnExit();
    try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
      try (FileChannel channel = f.getChannel()) {
        MappedByteBuffer bb = channel.map(MapMode.READ_WRITE, 0, 1);
        assertTrue(MMFUtils.unMapBuffer(bb, channel.getClass()));
      }
    }
  }

  @Test
  void testNullByteBuffer() throws FileNotFoundException, IOException {
    assertFalse(MMFUtils.unMapBuffer(null, FileChannel.class));
  }
}
