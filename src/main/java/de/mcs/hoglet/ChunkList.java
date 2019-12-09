package de.mcs.hoglet;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import de.mcs.hoglet.vlog.VLog;
import de.mcs.hoglet.vlog.VLogList;

public class ChunkList implements Closeable {

  private String collection;
  private byte[] key;
  private List<ChunkEntry> chunks;
  private HogletDB hogletDB;

  private ChunkList() {
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
   * @param key the key to set
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
   * @param collection the collection to set
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
      vlog.put(getCollection(), key, chunkNumber, chunk, operation)
    }
  }

  @Override
  public void close() throws IOException {
    // TODO implement saving metadata
  }

  public ChunkList withHogletDB(HogletDB hogletDB) {
    this.hogletDB = hogletDB;
    return this;
  }

}
