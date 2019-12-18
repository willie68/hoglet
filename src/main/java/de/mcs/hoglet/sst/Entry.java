/**
 * 
 */
package de.mcs.hoglet.sst;

/**
 * This class encasulate a database entry with key and value
 * 
 * @author w.klaas
 *
 */
public class Entry {
  private MapKey key;
  private byte[] value;

  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * @param value
   *          the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  /**
   * @param value
   *          the value to set
   */
  public Entry withValue(byte[] value) {
    setValue(value);
    return this;
  }

  /**
   * @return the key
   */
  public MapKey getKey() {
    return key;
  }

  /**
   * @param key
   *          the key to set
   */
  public void setKey(MapKey key) {
    this.key = key;
  }

  /**
   * @param key
   *          the key to set
   * @return
   */
  public Entry withKey(MapKey key) {
    setKey(key);
    return this;
  }
}
