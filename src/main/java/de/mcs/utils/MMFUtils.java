/**
 * 
 */
package de.mcs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * @author w.klaas
 *
 */
public class MMFUtils {
  private MMFUtils() {
  }

  private static boolean systemCapableOfUnmapMMF = false;
  private static boolean initialised = false;

  public static boolean isSystemCapableOfUnmapMMF() throws FileNotFoundException, IOException {
    if (!initialised) {
      File file = File.createTempFile("test", ".mmf");
      file.deleteOnExit();
      try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
        try (FileChannel channel = f.getChannel()) {
          MappedByteBuffer bb = channel.map(MapMode.READ_WRITE, 0, 1);
          systemCapableOfUnmapMMF = unMapBuffer(bb, channel.getClass());
        }
      }
      initialised = true;
    }
    return systemCapableOfUnmapMMF;
  }

  public static boolean unMapBuffer(MappedByteBuffer buffer, Class channelClass) {
    if (buffer == null) {
      return false;
    }

    try {
      Method unmap = channelClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
      unmap.setAccessible(true);
      unmap.invoke(channelClass, buffer);
      return true;
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return false;
  }

}
