package de.mcs.hoglet;

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

import java.io.StringWriter;

import de.mcs.utils.GsonUtils;

/**
 * @author wklaa_000
 *
 */
public class Options {

  /**
   * create a new option object with defaults
   * 
   * @return option object with defaults
   */
  public static Options defaultOptions() {
    return new Options().withVCntCompressAge(0).withVCntDeleteTreshHold(10).withVLogAge(1 * 60 * 60 * 1000)
        .withVlogMaxChunkCount(10000).withVlogMaxSize(100 * 1024 * 1024).withVlogMaxFileCount(10)
        .withChunkSize(1024 * 1024);
  }

  /**
   * path to the database folder
   */
  private String path;

  /**
   * blobs will be chuncked with this size. Defualt value is 1MB.
   */
  private int chunkSize;

  /**
   * how much is the delete treshhold
   * if the deleted data bytes are > then the treshhold (in percent) than the container will be compacted.
   */
  int vCntDeleteTreshHold;

  /**
   * the age of the container after that this container will be compressed. in days. 0= no compression
   */
  long vCntCompressAge;

  /**
   * mode of the compression
   */
  int vCntCompressionMode;

  /**
   * maximum size of a container file. If the file is > than that, the container file will be marked as read only
   */
  long vCntMaxSize;

  /**
   * maximum count of chunks in a container file. If the count is > than that, the container file will be marked for
   * compacting and marked as read only
   */
  long vCntMaxChunkCount;

  /**
   * maximum count of vLog files. If the count is > than that, a new vLog file will not be created.
   * Any put will than have to wait, until another vlog file is ready for taking this request or a
   * older vlog file has been compacted.
   */
  private long vlogMaxFileCount;

  /**
   * maximum size of a vLog file. If the file is > than that, the vLog file will be marked for compacting and as read
   * only
   */
  long vlogMaxSize;

  /**
   * maximum count of chunks in a vLog file. If the count is > than that, the vLog file will be marked for compacting
   * and
   * as read only
   */
  long vlogMaxChunkCount;

  /**
   * if the last write access is oldeer than that, the vLog file will be marked for compacting and as read only
   */
  long vLogAge;

  /**
   * @return the vCntDeleteTreshHold
   */
  public int getvCntDeleteTreshHold() {
    return vCntDeleteTreshHold;
  }

  /**
   * @param vCntDeleteTreshHold
   *          the vCntDeleteTreshHold to set
   * @return
   */
  public void setvCntDeleteTreshHold(int vCntDeleteTreshHold) {
    this.vCntDeleteTreshHold = vCntDeleteTreshHold;
  }

  /**
   * @param vCntDeleteTreshHold
   *          the vCntDeleteTreshHold to set
   * @return
   */
  public Options withVCntDeleteTreshHold(int vCntDeleteTreshHold) {
    this.vCntDeleteTreshHold = vCntDeleteTreshHold;
    return this;
  }

  /**
   * @return the vCntCompressAge
   */
  public long getvCntCompressAge() {
    return vCntCompressAge;
  }

  /**
   * @param vCntCompressAge
   *          the vCntCompressAge to set
   */
  public void setvCntCompressAge(long vCntCompressAge) {
    this.vCntCompressAge = vCntCompressAge;
  }

  /**
   * @param vCntCompressAge
   *          the vCntCompressAge to set
   */
  public Options withVCntCompressAge(long vCntCompressAge) {
    this.vCntCompressAge = vCntCompressAge;
    return this;
  }

  /**
   * @return the vlogMaxSize
   */
  public long getVlogMaxSize() {
    return vlogMaxSize;
  }

  /**
   * @param vlogMaxSize
   *          the vlogMaxSize to set
   */
  public void setVlogMaxSize(long vlogMaxSize) {
    this.vlogMaxSize = vlogMaxSize;
  }

  /**
   * @param vlogMaxSize
   *          the vlogMaxSize to set
   */
  public Options withVlogMaxSize(long vlogMaxSize) {
    this.vlogMaxSize = vlogMaxSize;
    return this;
  }

  /**
   * @return the vlogMaxChunkCount
   */
  public long getVlogMaxChunkCount() {
    return vlogMaxChunkCount;
  }

  /**
   * @param vlogMaxChunkCount
   *          the vlogMaxChunkCount to set
   */
  public void setVlogMaxChunkCount(long vlogMaxChunkCount) {
    this.vlogMaxChunkCount = vlogMaxChunkCount;
  }

  /**
   * @param vlogMaxChunkCount
   *          the vlogMaxChunkCount to set
   */
  public Options withVlogMaxChunkCount(long vlogMaxChunkCount) {
    this.vlogMaxChunkCount = vlogMaxChunkCount;
    return this;
  }

  /**
   * @return the vLogAge
   */
  public long getVLogAge() {
    return vLogAge;
  }

  /**
   * @param vLogAge
   *          the vLogAge to set
   */
  public void setVLogAge(long vLogAge) {
    this.vLogAge = vLogAge;
  }

  /**
   * @param vLogAge
   *          the vLogAge to set
   */
  public Options withVLogAge(long vLogAge) {
    this.vLogAge = vLogAge;
    return this;
  }

  /**
   * @return the path
   */
  public String getPath() {
    return path;
  }

  /**
   * @param path
   *          the path to set
   * @return
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * @param path
   *          the path to set
   * @return
   */
  public Options withPath(String path) {
    this.path = path;
    return this;
  }

  /**
   * @return the vlogMaxCount
   */
  public long getVlogMaxFileCount() {
    return vlogMaxFileCount;
  }

  /**
   * @param vlogMaxFileCount
   *          the vlogMaxCount to set
   * @return
   */
  public void setVlogMaxFileCount(long vlogMaxFileCount) {
    this.vlogMaxFileCount = vlogMaxFileCount;
  }

  /**
   * @param vlogMaxFileCount
   *          the vlogMaxCount to set
   * @return
   */
  public Options withVlogMaxFileCount(long vlogMaxFileCount) {
    this.vlogMaxFileCount = vlogMaxFileCount;
    return this;
  }

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toYamlString() {
    StringWriter writer = new StringWriter();
    GsonUtils.getYamlMapper().dump(this, writer);
    return writer.toString();
  }

  /**
   * @return the vlogChunkSize
   */
  public int getChunkSize() {
    return chunkSize;
  }

  /**
   * @param chunkSize
   *          the vlogChunkSize to set
   * @return
   */
  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * @param chunkSize
   *          the vlogChunkSize to set
   * @return
   */
  public Options withChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
    return this;
  }

  /**
   * @return the vCntMaxSize
   */
  public long getvCntMaxSize() {
    return vCntMaxSize;
  }

  /**
   * @param vCntMaxSize
   *          the vCntMaxSize to set
   * @return
   */
  public void setVCntMaxSize(long vCntMaxSize) {
    this.vCntMaxSize = vCntMaxSize;
  }

  /**
   * @param vCntMaxSize
   *          the vCntMaxSize to set
   * @return
   */
  public Options withVCntMaxSize(long vCntMaxSize) {
    this.vCntMaxSize = vCntMaxSize;
    return this;
  }

  /**
   * @return the vCntMaxChunkCount
   */
  public long getVCntMaxChunkCount() {
    return vCntMaxChunkCount;
  }

  /**
   * @param vCntMaxChunkCount
   *          the vCntMaxChunkCount to set
   * @return
   */
  public void setVCntMaxChunkCount(long vCntMaxChunkCount) {
    this.vCntMaxChunkCount = vCntMaxChunkCount;
  }

  /**
   * @param vCntMaxChunkCount
   *          the vCntMaxChunkCount to set
   * @return
   */
  public Options withVCntMaxChunkCount(long vCntMaxChunkCount) {
    this.vCntMaxChunkCount = vCntMaxChunkCount;
    return this;
  }
}
