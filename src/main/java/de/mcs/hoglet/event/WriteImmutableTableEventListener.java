/**
 * 
 */
package de.mcs.hoglet.event;

import com.google.common.eventbus.Subscribe;

import de.mcs.hoglet.HogletDB;

/**
 * @author w.klaas
 *
 */
public class WriteImmutableTableEventListener {

  public static class WriteImmutableTableEvent {

  }

  private HogletDB hogletDB;

  public WriteImmutableTableEventListener(HogletDB hogletDB) {
    this.hogletDB = hogletDB;
  }

  @Subscribe
  public void writeImmutableTable(WriteImmutableTableEvent event) {
    hogletDB.writeImmutableTable();
  }
}
