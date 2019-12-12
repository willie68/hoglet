package de.mcs.hoglet.sst;

public interface MemoryTable {

  boolean containsKey(String collection, byte[] key);

  byte[] get(String collection, byte[] key);

  byte[] remove(String collection, byte[] key);

  byte[] add(String collection, byte[] key, byte[] bytes);

  int size();

  boolean isAvailbleForWriting();

}
