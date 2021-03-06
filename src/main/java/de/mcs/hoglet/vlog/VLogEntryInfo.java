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

import de.mcs.utils.ByteArrayUtils;

/**
 * @author w.klaas
 *
 */
public class VLogEntryInfo {
  long start;
  long startBinary;
  long end;
  byte[] hash;
  private String vLogName;

  /**
   * @return the start
   */
  public long getStart() {
    return start;
  }

  /**
   * @return the startBinary
   */
  public long getStartBinary() {
    return startBinary;
  }

  /**
   * @return the hash
   */
  public byte[] getHash() {
    return hash;
  }

  public int getBinarySize() {
    return (int) (end - startBinary + 1);
  }

  public long getEnd() {
    return end;
  }

  @Override
  public String toString() {
    return String.format("start: %d, bin: %d, end: %d, hash: %s", start, startBinary, end,
        ByteArrayUtils.bytesAsHexString(hash));
  }

  public int getDescriptionSize() {
    return (int) (startBinary - start);
  }

  /**
   * @param start
   *          the start to set
   * @return
   */
  public VLogEntryInfo setStart(long start) {
    this.start = start;
    return this;
  }

  /**
   * @param startBinary
   *          the startBinary to set
   * @return
   */
  public VLogEntryInfo setStartBinary(long startBinary) {
    this.startBinary = startBinary;
    return this;
  }

  /**
   * @param end
   *          the end to set
   * @return
   */
  public VLogEntryInfo setEnd(long end) {
    this.end = end;
    return this;
  }

  /**
   * @param hash
   *          the hash to set
   * @return
   */
  public VLogEntryInfo setHash(byte[] hash) {
    this.hash = hash;
    return this;
  }

  /**
   * @return the vLogName
   */
  public String getvLogName() {
    return vLogName;
  }

  /**
   * @param vLogName the vLogName to set
   */
  public void setvLogName(String vLogName) {
    this.vLogName = vLogName;
  }

}
