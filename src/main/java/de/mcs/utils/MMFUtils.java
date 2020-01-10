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
