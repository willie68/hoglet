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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import de.mcs.utils.ByteArrayUtils;

class TestSSTableIndex {

  private SSTableIndex index;
  private List<byte[]> myKeys;

  @BeforeEach
  void setUp() throws Exception {
  }

  @AfterEach
  void tearDown() throws Exception {
  }

  @Order(1)
  @Test
  void test() {
    index = new SSTableIndex().withCacheSize(1000);
    assertEquals(1000, index.getCacheSize());
    assertFalse(index.hasEntry(1001));

    assertFalse(index.hasEntry(0));

    myKeys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      byte[] key = ByteArrayUtils.longToBytes(i);
      myKeys.add(key);
    }

    myKeys.sort(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return Arrays.compare(o1, o2);
      }
    });

    for (int i = 0; i < myKeys.size(); i++) {

      byte[] bs = myKeys.get(i);

      MapKey key = MapKey.buildPrefixedKey("MCS", bs);
      index.setEntry(i, i * 1024, key);

      assertTrue(index.hasEntry(i));

      int startPosition = index.getStartPosition(key);
      assertEquals(i * 1024, index.getStartPosition(key), "error on key " + i + " " + startPosition);
    }
  }

  @Order(2)
  @Test
  void testJson() {
    if (index == null) {
      test();
    }
    String json = index.toJson();
    SSTableIndex index2 = SSTableIndex.fromJson(json);

    for (int i = 0; i < myKeys.size(); i++) {

      byte[] bs = myKeys.get(i);

      MapKey key = MapKey.buildPrefixedKey("MCS", bs);

      assertTrue(index.hasEntry(i));
      assertTrue(index2.hasEntry(i));

      int startPosition = index2.getStartPosition(key);
      assertEquals(i * 1024, index2.getStartPosition(key), "error on key " + i + " " + startPosition);
    }
  }

  @Test
  void testCalcCacheSize() {
    assertEquals(100, SSTableIndex.calcCacheSize(1000));
    assertEquals(100, SSTableIndex.calcCacheSize(10000));
    assertEquals(200, SSTableIndex.calcCacheSize(20000));
    assertEquals(500, SSTableIndex.calcCacheSize(50000));
    assertEquals(1000, SSTableIndex.calcCacheSize(100000));
    assertEquals(1000, SSTableIndex.calcCacheSize(1000000));
    assertEquals(1000, SSTableIndex.calcCacheSize(10000000));
  }
}
