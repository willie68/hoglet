package de.mcs.hoglet.sst;

import java.util.Arrays;

import de.mcs.utils.GsonUtils;

public class SSTableIndex {

  private int[] indexList;
  private MapKey[] mapkeyList;
  private int cacheSize;

  public SSTableIndex() {
    setCacheSize(0);
  }

  public SSTableIndex withCacheSize(int cacheSize) {
    setCacheSize(cacheSize);
    return this;
  }

  /**
   * @return the cacheSize
   */
  public int getCacheSize() {
    return cacheSize;
  }

  /**
   * @param cacheSize
   *          the cacheSize to set
   */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    indexList = new int[cacheSize];
    mapkeyList = new MapKey[cacheSize];
    Arrays.fill(indexList, -1);
  }

  public boolean hasEntry(int index) {
    if (index > cacheSize) {
      return false;
    }
    return indexList[index] >= 0;
  }

  public void setEntry(int index, int savePosition, MapKey key) {
    if (index > cacheSize) {
      return;
    }
    indexList[index] = savePosition;
    mapkeyList[index] = key;
  }

  public int getStartPosition(MapKey mapKey) {
    int startPosition = 0;
    for (int i = 0; i < indexList.length; i++) {
      if (indexList[i] < 0) {
        break;
      }
      if (mapKey.compareTo(mapkeyList[i]) >= 0) {
        startPosition = indexList[i];
      } else {
        return startPosition;
      }
    }
    return startPosition;
  }

  public String toJson() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public static SSTableIndex fromJson(String json) {
    return GsonUtils.getJsonMapper().fromJson(json, SSTableIndex.class);
  }
}
