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

import java.nio.charset.StandardCharsets;

import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class VLogEntryInfo {

  /**
   * creating a VLogEntryInfo from json string
   * 
   * @param json
   *          the json representation
   * @return VLogEntryInfo
   */
  public static VLogEntryInfo fromBytes(byte[] json) {
    return GsonUtils.getJsonMapper().fromJson(new String(json, StandardCharsets.UTF_8), VLogEntryInfo.class);
  }

  /**
   * public creation of new VLogEntryInfo
   * 
   * @return new empty VLogEntryInfo
   */
  public static VLogEntryInfo newVLogEntryInfo() {
    return new VLogEntryInfo();
  }

  long start;
  long startBinary;
  long end;
  byte[] hash;
  String vLogName;
  byte[] value;

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
    return String.format("start: %d, bin: %d, end: %d, hash: %s, file: %s", start, startBinary, end, hash, vLogName);
  }

  public int getDescriptionSize() {
    return (int) (startBinary - start);
  }

  /**
   * @param start
   *          the start to set
   * @return
   */
  public void setStart(long start) {
    this.start = start;
  }

  /**
   * @param startBinary
   *          the startBinary to set
   * @return
   */
  public void setStartBinary(long startBinary) {
    this.startBinary = startBinary;
  }

  /**
   * @param end
   *          the end to set
   * @return
   */
  public void setEnd(long end) {
    this.end = end;
  }

  /**
   * @param hash
   *          the hash to set
   * @return
   */
  public void setHash(byte[] hash) {
    this.hash = hash;
  }

  /**
   * @return the vLogName
   */
  public String getvLogName() {
    return vLogName;
  }

  /**
   * @param vLogName
   *          the vLogName to set
   */
  public void setvLogName(String vLogName) {
    this.vLogName = vLogName;
  }

  /**
   * getting the representing json from this object
   * 
   * @return String
   */
  public byte[] asBytes() {
    return GsonUtils.getJsonMapper().toJson(this).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * @param start
   *          the start to set
   * @return
   */
  public VLogEntryInfo withStart(long start) {
    this.setStart(start);
    return this;
  }

  /**
   * @param startBinary
   *          the startBinary to set
   * @return
   */
  public VLogEntryInfo withStartBinary(long startBinary) {
    this.setStartBinary(startBinary);
    return this;
  }

  /**
   * @param end
   *          the end to set
   * @return
   */
  public VLogEntryInfo withEnd(long end) {
    this.setEnd(end);
    return this;
  }

  /**
   * @param hash
   *          the hash to set
   * @return
   */
  public VLogEntryInfo withHash(byte[] hash) {
    this.setHash(hash);
    return this;
  }

  /**
   * @param vLogName
   *          the vLogName to set
   */
  public VLogEntryInfo withVLogName(String vLogName) {
    this.setvLogName(vLogName);
    return this;
  }

  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * @param value
   *          the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  /**
   * @param value
   *          the value to set
   * @return
   */
  public VLogEntryInfo withValue(byte[] value) {
    this.setValue(value);
    return this;
  }

}
