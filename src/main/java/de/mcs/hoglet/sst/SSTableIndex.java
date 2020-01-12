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

import java.util.Arrays;

import de.mcs.utils.GsonUtils;

public class SSTableIndex {

  private static final int MAX_CACHE_SIZE = 1000;

  public static int calcCacheSize(long keyCount) {
    int cacheSize = (int) Math.max(100, keyCount / 100);
    cacheSize = (int) Math.min(cacheSize, MAX_CACHE_SIZE);
    return cacheSize;
  }

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
