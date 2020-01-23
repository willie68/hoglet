/**
 * 
 */
package de.mcs.hoglet;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author w.klaas
 *
 */
public class HogletOracle {

  private AtomicLong id = new AtomicLong();

  public static HogletOracle newHogletOracle(long id) {
    return new HogletOracle().withId(id);
  }

  private HogletOracle() {
    init();
  }

  private void init() {

  }

  /**
   * @return the id
   */
  public long getId() {
    return id.get();
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId(long id) {
    this.id.set(id);
  }

  public HogletOracle withId(long id) {
    setId(id);
    return this;
  }

  public long getNextId() {
    return id.getAndIncrement();
  }

}
