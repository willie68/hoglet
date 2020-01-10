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
package de.mcs.hoglet.sst;

import java.util.Date;

import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.utils.GsonUtils;

/**
 * This class contains the status of a sst table file
 * 
 * @author wklaa_000
 *
 */
public class SSTStatus {

  private byte[] bloomfilter;
  private long chunkCount;
  private long createdAt;
  private VLogEntryInfo lastVLogEntry;

  /**
   * @return the bloomfilter
   */
  public byte[] getBloomfilter() {
    return bloomfilter;
  }

  /**
   * @param bloomfilter
   *          the bloomfilter to set
   */
  public void setBloomfilter(byte[] bloomfilter) {
    this.bloomfilter = bloomfilter;
  }

  /**
   * @param bloomfilter
   *          the bloomfilter to set
   * @return
   */
  public SSTStatus withBloomfilter(byte[] bloomfilter) {
    setBloomfilter(bloomfilter);
    return this;
  }

  /**
   * @return the chunkCount
   */
  public long getChunkCount() {
    return chunkCount;
  }

  /**
   * @param chunkCount
   *          the chunkCount to set
   */
  public void setChunkCount(long chunkCount) {
    this.chunkCount = chunkCount;
  }

  /**
   * @param chunkCount
   *          the chunkCount to set
   * @return
   */
  public SSTStatus withChunkCount(long chunkCount) {
    this.setChunkCount(chunkCount);
    return this;
  }

  /**
   * @return the createdAt
   */
  public Date getCreatedAt() {
    return new Date(createdAt);
  }

  /**
   * @param createdAt
   *          the createdAt to set
   */
  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt.getTime();
  }

  /**
   * @param createdAt
   *          the createdAt to set
   * @return
   */
  public SSTStatus withCreatedAt(Date createdAt) {
    this.setCreatedAt(createdAt);
    return this;
  }

  /**
   * @return the lastVLogName
   */
  public VLogEntryInfo getLastVLogEntry() {
    return lastVLogEntry;
  }

  /**
   * @param lastVLogName
   *          the lastVLogName to set
   */
  public void setLastVLogEntry(VLogEntryInfo lastVLogEntry) {
    this.lastVLogEntry = lastVLogEntry;
  }

  /**
   * @param lastVLogName
   *          the lastVLogName to set
   * @return
   */
  public SSTStatus withLastVLogEntry(VLogEntryInfo lastVLogEntry) {
    this.setLastVLogEntry(lastVLogEntry);
    return this;
  }

  public String toJson() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public static SSTStatus fromJson(String json) {
    return GsonUtils.getJsonMapper().fromJson(json, SSTStatus.class);
  }
}
