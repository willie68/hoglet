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
