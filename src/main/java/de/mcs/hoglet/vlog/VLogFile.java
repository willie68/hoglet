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
package de.mcs.hoglet.vlog;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.io.input.BoundedInputStream;

import de.mcs.hoglet.HogletDBException;
import de.mcs.hoglet.Options;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.HashUtils.Algorithm;
import de.mcs.utils.io.RandomAccessInputStream;
import de.mcs.utils.logging.Logger;

/**
 * This is the implementing class for a value log file, all binary datas will be stored simply in sequential order.
 * And there are as many v log files open as parallel tasks writing to them.
 * 
 * @author wklaa_000
 *
 */
public class VLogFile implements Closeable {

  private static final long MAX_VLOG_SIZE = (1024L * 1024L * 1024L * 2L) - 1L;
  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File vLogFile;
  private FileChannel fileChannel;
  private RandomAccessFile raf;
  private Options options;
  private int chunkCount;
  private boolean readOnly;
  private MessageDigest messageDigest;

  public static File getFilePathName(File path, int number) {
    String internalName = String.format("vlog_%04d.vlog", number);
    return new File(path, internalName);
  }

  private VLogFile() {
    chunkCount = -1;
    messageDigest = Algorithm.SHA_256.getMessageDigest();
  }

  public VLogFile(Options options, int number) throws IOException {
    this();
    this.options = options;
    this.vLogFile = getFilePathName(new File(options.getPath()), number);
    init();
  }

  public VLogFile(Options options, File file) throws IOException {
    this();
    this.options = options;
    this.vLogFile = file;
    init();
  }

  private void init() throws IOException {
    internalName = vLogFile.getName();
    if (vLogFile.exists()) {
      loadLogFile();
    } else {
      initLogFile();
    }
  }

  private void loadLogFile() throws IOException {
    log.debug("loading vlog file: %s", internalName);
    raf = new RandomAccessFile(vLogFile, "r");
    raf.seek(raf.length());
    fileChannel = raf.getChannel();
    chunkCount = -1;
    readOnly = true;
  }

  private void initLogFile() throws IOException {
    log.debug("creating new vlog file: %s", internalName);
    raf = new RandomAccessFile(vLogFile, "rw");
    raf.setLength(options.getVlogMaxSize());
    raf.seek(0);
    fileChannel = raf.getChannel();
    chunkCount = 0;
    readOnly = false;
  }

  public String getName() {
    return internalName;
  }

  @Override
  public void close() throws IOException {
    if ((fileChannel != null) && fileChannel.isOpen()) {
      fileChannel.force(true);
      long position = fileChannel.position();
      if (!readOnly) {
        raf.setLength(position);
      }
      fileChannel.close();
    }
    raf.close();
  }

  public VLogEntryInfo put(String collection, byte[] key, int chunknumber, byte[] chunk) throws IOException {
    byte[] collectionBytes = collection.getBytes(StandardCharsets.UTF_8);
    if (collectionBytes.length > VLogDescriptor.KEY_MAX_LENGTH) {
      throw new HogletDBException("Illegal collection length.");
    }
    if (key.length > VLogDescriptor.KEY_MAX_LENGTH) {
      throw new HogletDBException("Illegal key length.");
    }
    if (!isAvailbleForWriting()) {
      throw new HogletDBException(String.format("VLogfile %s is not available for writing.", internalName));
    }
    byte[] digest = new byte[2];
    // calculating hash of chunk
    CRC32 crc32 = new CRC32();
    crc32.update(chunk);
    digest = ByteArrayUtils.longToBytes(crc32.getValue());

    VLogEntryInfo info = new VLogEntryInfo();
    info.start = fileChannel.position();
    info.hash = digest;

    VLogDescriptor vlogDescriptor = new VLogDescriptor();
    vlogDescriptor.collectionBytes = collectionBytes;
    vlogDescriptor.key = key;
    vlogDescriptor.chunkNumber = chunknumber;
    vlogDescriptor.hash = digest;
    vlogDescriptor.length = chunk.length;
    // write the binary data
    fileChannel.write(vlogDescriptor.getBytes());
    info.startBinary = fileChannel.position();
    fileChannel.write(ByteBuffer.wrap(chunk));
    info.end = fileChannel.position() - 1;
    fileChannel.force(false);
    info.setvLogName(getName());
    chunkCount++;
    return info;
  }

  public VLogEntryDescription getDescription(long offset, int size) throws IOException {
    try (BufferedInputStream in = new BufferedInputStream(new RandomAccessInputStream(vLogFile, offset),
        options.getChunkSize())) {
      byte[] buffer = new byte[size];
      int read = in.read(buffer);
      if (read != size) {
        throw new HogletDBException("readed buffer size is not equals to size");
      }
      VLogDescriptor descriptor = VLogDescriptor.fromBytes(buffer);
      return VLogTransformer.transformDescriptor2Entry(descriptor);
    }
  }

  public InputStream get(long offset, int size) throws IOException {
    return new BufferedInputStream(new BoundedInputStream(new RandomAccessInputStream(vLogFile, offset), size),
        options.getChunkSize());
  }

  public byte[] getValue(long offset, int size) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(vLogFile, "r")) {
      raf.seek(offset);
      byte[] buffer = new byte[size];
      raf.read(buffer);
      return buffer;
    }
  }

  public long getSize() {
    return vLogFile.length();
  }

  public boolean isAvailbleForWriting() {
    if (readOnly) {
      return false;
    }
    if (getSize() > options.getVlogMaxSize()) {
      return false;
    }
    if (getChunkCount() + 1 > options.getVlogMaxChunkCount()) {
      return false;
    }
    return true;
  }

  /**
   * @return the chunkCount
   */
  public int getChunkCount() {
    return chunkCount;
  }

  /**
   * @return the readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  public VLogFile setReadOnly(boolean readonly) {
    this.readOnly = readonly;
    return this;
  }

  public File getFile() {
    return vLogFile;
  }

  public Iterator<VLogEntryDescription> iterator() throws IOException {
    List<VLogEntryDescription> entryInfos = new ArrayList<>();

    try (RandomAccessInputStream in = new RandomAccessInputStream(vLogFile)) {
      try (BufferedInputStream input = new BufferedInputStream(in)) {
        boolean markerFound = false;
        long position = 0;
        while (input.available() > 0) {
          markerFound = true;
          long start = position;
          byte[] next = input.readNBytes(4);
          if (next.length != 4) {
            markerFound = false;
          }
          position += 4;
          if (!Arrays.equals(VLogDescriptor.DOC_START, next)) {
            markerFound = false;
          } else {
            long startDescription = position;
            byte[] descriptorArray = input.readNBytes(VLogDescriptor.lengthWithoutStart());
            if (descriptorArray.length != VLogDescriptor.lengthWithoutStart()) {
              throw new IOException("error reading description.");
            }
            position += descriptorArray.length;
            VLogDescriptor descriptor = VLogDescriptor.fromBytesWithoutStart(descriptorArray);
            if (descriptor == null) {
              throw new IOException("length not ok");
            } else {
              // System.out.println("entry found: \r\n");
              VLogEntryDescription info = new VLogEntryDescription();
              info.chunkNumber = descriptor.chunkNumber;
              info.containerName = getName();
              info.end = position + descriptor.length - 1;
              info.collection = new String(descriptor.collectionBytes, StandardCharsets.UTF_8);
              info.hash = descriptor.hash;
              info.key = descriptor.key;
              info.length = descriptor.length;
              info.start = start;
              info.startBinary = position;
              info.startDescription = startDescription;
              entryInfos.add(info);

              long bytesToSkip = descriptor.length;
              while ((bytesToSkip > 0) && (input.available() > 0)) {
                long skip = input.skip(bytesToSkip);
                if (skip < 0) {
                  throw new IOException("vLog not correctly padded.");
                }
                bytesToSkip -= skip;
                position += skip;
              }
            }
          }
          if (!markerFound) {
          }
        }
      }
    }
    return entryInfos.iterator();
  }

}
