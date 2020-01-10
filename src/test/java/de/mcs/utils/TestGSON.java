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
package de.mcs.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.ChunkEntry;
import de.mcs.hoglet.ChunkList;
import de.mcs.utils.GsonUtils;

class TestGSON {

  @BeforeEach
  void setUp() throws Exception {
  }

  @Test
  void test() {
    byte[] key = new byte[] { 0x00, 0x01, 0x01, 0x03 };
    ChunkList list = ChunkList.newChunkList().withCollection("easy").withKey(key);
    ChunkEntry entry = new ChunkEntry().setChunkNumber(1).setContainerName("cont0001").setHash(key).setLength(12345)
        .setStart(0).setStartBinary(12);
    list.getChunks().add(entry);
    String json = GsonUtils.getJsonMapper().toJson(list);
    System.out.println(json);
  }

}
