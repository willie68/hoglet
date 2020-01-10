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
package de.mcs.hoglet.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.junit.jupiter.api.Test;

import de.mcs.hoglet.vlog.VLogEntryInfo;

class TestSSTStatus {

  @Test
  void test() {
    byte[] ba = new byte[1024];
    new Random().nextBytes(ba);

    byte[] hash = new byte[128];
    new Random().nextBytes(hash);

    Date createdAt = new Date();
    VLogEntryInfo entry = new VLogEntryInfo().withEnd(10).withHash(hash).withStart(0).withStartBinary(5)
        .withVLogName("00000008.vlog");

    SSTStatus status = new SSTStatus().withBloomfilter(ba).withChunkCount(1234).withCreatedAt(createdAt)
        .withLastVLogEntry(entry);

    String json = status.toJson();
    System.out.println(json);

    SSTStatus newStatus = SSTStatus.fromJson(json);

    assertTrue(Arrays.equals(status.getBloomfilter(), newStatus.getBloomfilter()));
    assertEquals(status.getChunkCount(), newStatus.getChunkCount());
    assertTrue(status.getCreatedAt().compareTo(newStatus.getCreatedAt()) == 0);
    VLogEntryInfo newEntry = newStatus.getLastVLogEntry();

    assertNotNull(entry);
    assertEquals(entry.getEnd(), newEntry.getEnd());
    assertTrue(Arrays.equals(entry.getHash(), newEntry.getHash()));
    assertEquals(entry.getStart(), newEntry.getStart());
    assertEquals(entry.getStartBinary(), newEntry.getStartBinary());
    assertEquals(entry.getvLogName(), newEntry.getvLogName());
    assertEquals(entry.getBinarySize(), newEntry.getBinarySize());

  }

}
