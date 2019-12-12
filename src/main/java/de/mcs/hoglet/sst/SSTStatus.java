/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: SSTStatus.java
 * EMail: W.Klaas@gmx.de
 * Created: 12.12.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.hoglet.sst;

import java.util.Date;

/**
 * This class contains the status of a sst table file
 * @author wklaa_000
 *
 */
public class SSTStatus {
  private byte[] bloomfilter;
  private long chunkCount;
  private Date createdAt;

  /**
   * @return the bloomfilter
   */
  public byte[] getBloomfilter() {
    return bloomfilter;
  }

  /**
   * @param bloomfilter the bloomfilter to set
   */
  public void setBloomfilter(byte[] bloomfilter) {
    this.bloomfilter = bloomfilter;
  }

  /**
   * @param bloomfilter the bloomfilter to set
   * @return 
   */
  public SSTStatus withBloomfilter(byte[] bloomfilter) {
    this.bloomfilter = bloomfilter;
    return this;
  }

  /**
   * @return the chunkCount
   */
  public long getChunkCount() {
    return chunkCount;
  }

  /**
   * @param chunkCount the chunkCount to set
   */
  public void setChunkCount(long chunkCount) {
    this.chunkCount = chunkCount;
  }

  /**
   * @param chunkCount the chunkCount to set
   * @return 
   */
  public SSTStatus withChunkCount(long chunkCount) {
    this.chunkCount = chunkCount;
    return this;
  }

  /**
   * @return the createdAt
   */
  public Date getCreatedAt() {
    return createdAt;
  }

  /**
   * @param createdAt the createdAt to set
   */
  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  /**
   * @param createdAt the createdAt to set
   * @return 
   */
  public SSTStatus withCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
    return this;
  }

}
