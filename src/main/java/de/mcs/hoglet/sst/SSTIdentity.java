/**
 * 
 */
package de.mcs.hoglet.sst;

/**
 * @author w.klaas
 *
 */
public class SSTIdentity {
  private int level;
  private int number;

  public static SSTIdentity newSSTIdentity() {
    return new SSTIdentity();
  }

  /**
   * @return the level
   */
  public int getLevel() {
    return level;
  }

  /**
   * @param level
   *          the level to set
   */
  public void setLevel(int level) {
    this.level = level;
  }

  public SSTIdentity withLevel(int level) {
    setLevel(level);
    return this;
  }

  /**
   * @return the number
   */
  public int getNumber() {
    return number;
  }

  /**
   * @param number
   *          the number to set
   */
  public void setNumber(int number) {
    this.number = number;
  }

  public SSTIdentity withNumber(int number) {
    setNumber(number);
    return this;
  }

}
