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
