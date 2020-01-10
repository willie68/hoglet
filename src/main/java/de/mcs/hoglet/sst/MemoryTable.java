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

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.vlog.VLogEntryInfo;

public interface MemoryTable extends Iterable<Entry> {

  boolean containsKey(String collection, byte[] key);

  byte[] get(String collection, byte[] key);

  byte[] remove(String collection, byte[] key);

  byte[] add(String collection, byte[] key, Operation operation, byte[] bytes);

  Operation getOperation(String collection, byte[] key);

  int size();

  boolean isAvailbleForWriting();

  void setLastVLogEntry(VLogEntryInfo info);

  VLogEntryInfo getLastVLogEntry();

}
