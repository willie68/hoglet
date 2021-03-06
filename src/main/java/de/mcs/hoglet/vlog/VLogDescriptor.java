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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import de.mcs.utils.ByteArrayUtils;

/**
 * @author w.klaas
 *
 */
public class VLogDescriptor {
  static final String VLOG_VERSION = "1";
  static final byte[] DOC_START = ("@@@" + VLOG_VERSION).getBytes(StandardCharsets.UTF_8);
  static final byte[] DOC_LIMITER = "#".getBytes(StandardCharsets.UTF_8);
  static final int KEY_MAX_LENGTH = 255;
  static final int HASH_LENGTH = ByteArrayUtils.LONGBYTES;

  // because of the headerstructure, 4 bytes DOC_START + 1 byte KEY_LENGTH +
  // COLLECTION + 1 byte KEY_LENGTH + KEY
  // itself + 4 bytes Chunknumber + 8 byte length + 32 byte hash + 1 byte
  // DOC_LIMITER
  private static final int HEADER_MAX_LENGTH = DOC_START.length + 1 + KEY_MAX_LENGTH + 1 + KEY_MAX_LENGTH + 4 + 8
      + HASH_LENGTH + DOC_LIMITER.length;

  byte[] collectionBytes;
  byte[] key;
  int chunkNumber;
  long length;
  byte[] hash;

  VLogDescriptor() {
    length = 0;
    hash = new byte[HASH_LENGTH];
  }

  String getHashAsString() {
    return ByteArrayUtils.bytesAsHexString(hash);
  }

  public ByteBuffer getBytes() {
    ByteBuffer header = ByteBuffer.allocateDirect(HEADER_MAX_LENGTH);
    header.rewind();
    header.put(DOC_START);
    header.put((byte) collectionBytes.length);
    header.put(collectionBytes);
    header.put((byte) key.length);
    header.put(key);
    header.putInt(chunkNumber);
    header.putLong(length);
    header.put(hash);
    header.put(DOC_LIMITER);
    byte[] padding = new byte[header.remaining()];
    Arrays.fill(padding, (byte) 0);
    header.put(padding);
    header.flip();
    return header;
  }

  public static VLogDescriptor fromBytes(byte[] byteArray) {
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    VLogDescriptor vLogPostFix = new VLogDescriptor();
    // don't read the doc seperator
    buffer.get(new byte[4]);
    int collectionLength = buffer.get();
    vLogPostFix.collectionBytes = new byte[collectionLength];
    buffer.get(vLogPostFix.collectionBytes);

    int keyLength = buffer.get();
    vLogPostFix.key = new byte[keyLength];
    buffer.get(vLogPostFix.key);

    vLogPostFix.chunkNumber = buffer.getInt();
    vLogPostFix.length = buffer.getLong();
    vLogPostFix.hash = new byte[HASH_LENGTH];
    buffer.get(vLogPostFix.hash);
    return vLogPostFix;
  }

  public static VLogDescriptor fromBytesWithoutStart(byte[] byteArray) {
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    return fromByteBufferWithoutStart(buffer);
  }

  public static VLogDescriptor fromByteBufferWithoutStart(ByteBuffer buffer) {
    VLogDescriptor vLogPostFix = new VLogDescriptor();
    int collectionLength = buffer.get();
    if (collectionLength < 1) {
      return null;
    }
    vLogPostFix.collectionBytes = new byte[collectionLength];
    buffer.get(vLogPostFix.collectionBytes);

    int keyLength = buffer.get();
    if (keyLength < 1) {
      return null;
    }
    vLogPostFix.key = new byte[keyLength];
    buffer.get(vLogPostFix.key);

    vLogPostFix.chunkNumber = buffer.getInt();
    if (vLogPostFix.chunkNumber < 0) {
      return null;
    }
    vLogPostFix.length = buffer.getLong();
    if (vLogPostFix.length < 0) {
      return null;
    }
    vLogPostFix.hash = new byte[HASH_LENGTH];
    buffer.get(vLogPostFix.hash);
    return vLogPostFix;
  }

  public static int length() {
    return HEADER_MAX_LENGTH;
  }

  public static int lengthWithoutStart() {
    return length() - DOC_START.length;
  }

  /**
   * @return the collectionBytes
   */
  public byte[] getCollectionBytes() {
    return collectionBytes;
  }

  /**
   * @param collectionBytes
   *          the collectionBytes to set
   * @return
   */
  public VLogDescriptor setCollectionBytes(byte[] collectionBytes) {
    this.collectionBytes = collectionBytes;
    return this;
  }

  /**
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * @param key
   *          the key to set
   * @return
   */
  public VLogDescriptor setKey(byte[] key) {
    this.key = key;
    return this;
  }

  /**
   * @return the chunkNumber
   */
  public int getChunkNumber() {
    return chunkNumber;
  }

  /**
   * @param chunkNumber
   *          the chunkNumber to set
   * @return
   */
  public VLogDescriptor setChunkNumber(int chunkNumber) {
    this.chunkNumber = chunkNumber;
    return this;
  }

  /**
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   * @return
   */
  public VLogDescriptor setLength(long length) {
    this.length = length;
    return this;
  }

  public byte[] getHash() {
    return hash;
  }

  /**
   * @param hash
   *          the hash to set
   * @return
   */
  public VLogDescriptor setHash(byte[] hash) {
    this.hash = hash;
    return this;
  }

}
