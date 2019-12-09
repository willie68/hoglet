package de.mcs.hoglet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import de.mcs.hoglet.vlog.VLog;
import de.mcs.hoglet.vlog.VLogEntryInfo;
import de.mcs.hoglet.vlog.VLogList;
import de.mcs.utils.GsonUtils;

public class ChunkList implements Closeable {

  private String collection;
  private byte[] key;
  private List<ChunkEntry> chunks;
  private transient HogletDB hogletDB;

  public ChunkList() {
    setChunks(new ArrayList<>());
  }

  public static ChunkList newChunkList() {
    return new ChunkList();
  }

  public ChunkList withCollection(String collection) {
    setCollection(collection);
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
   */
  public void setKey(byte[] key) {
    this.key = key;
  }

  /**
   * @return the collection
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection
   *          the collection to set
   */
  public void setCollection(String collection) {
    this.collection = collection;
  }

  public ChunkList withKey(byte[] key) {
    setKey(key);
    return this;
  }

  public void addChunk(int chunkNumber, byte[] chunk) throws IOException {
    if (chunkNumber < 0) {
      throw new IllegalArgumentException("chunknumber must be >=0");
    }
    VLogList vLogList = hogletDB.getVLogList();
    try (VLog vlog = vLogList.getNextAvailableVLog()) {
      VLogEntryInfo info = vlog.put(getCollection(), key, chunkNumber, chunk, Operation.ADD);
      ChunkEntry entry = new ChunkEntry().setChunkNumber(chunkNumber).setContainerName(vlog.getName())
          .setHash(info.getHash()).setLength(info.getBinarySize()).setStart(info.getStart())
          .setStartBinary(info.getStartBinary());
      this.getChunks().add(entry);
    }
  }

  /**
   * closing this chunk, adding a new value to the vlog with all chuns as json, adding then the key/value to the lsm db.
   */
  @Override
  public void close() throws IOException {
    String json = GsonUtils.getJsonMapper().toJson(this);
    hogletDB.put(key, json.getBytes(StandardCharsets.UTF_8));
  }

  public ChunkList withHogletDB(HogletDB hogletDB) {
    this.hogletDB = hogletDB;
    return this;
  }

  /**
   * @return the chunks
   */
  public List<ChunkEntry> getChunks() {
    return chunks;
  }

  /**
   * @param chunks
   *          the chunks to set
   */
  public void setChunks(List<ChunkEntry> chunks) {
    this.chunks = chunks;
  }

}
