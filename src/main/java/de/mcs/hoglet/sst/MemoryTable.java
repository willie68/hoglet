package de.mcs.hoglet.sst;

import de.mcs.hoglet.Operation;
import de.mcs.hoglet.vlog.VLogEntryInfo;

public interface MemoryTable extends Iterable<Entry> {

  boolean containsKey(String collection, byte[] key);

  byte[] get(String collection, byte[] key);

  byte[] remove(String collection, byte[] key);

  byte[] add(String collection, byte[] key, Operation operation, byte[] bytes);

  int size();

  boolean isAvailbleForWriting();

  void setLastVLogEntry(VLogEntryInfo info);

  VLogEntryInfo getLastVLogEntry();

}
