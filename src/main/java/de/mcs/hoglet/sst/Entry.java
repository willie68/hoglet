/**
 * 
 */
package de.mcs.hoglet.sst;

import de.mcs.hoglet.Operation;

/**
 * This class encasulate a database entry with key and value
 * 
 * @author w.klaas
 *
 */
public class Entry {
  private MapKey key;
  private Operation operation;
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

  /**
   * @return the operation
   */
  public Operation getOperation() {
    return operation;
  }

  /**
   * @param operation
   *          the operation to set
   */
  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  /**
   * @param operation
   *          the operation to set
   * @return
   */
  public Entry withOperation(Operation operation) {
    this.operation = operation;
    return this;
  }
}
