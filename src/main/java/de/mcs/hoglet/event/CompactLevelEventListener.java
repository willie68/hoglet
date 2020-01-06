/**
 * 
 */
package de.mcs.hoglet.event;

import com.google.common.eventbus.Subscribe;

import de.mcs.hoglet.sst.SSTableManager;

/**
 * @author w.klaas
 *
 */
public class CompactLevelEventListener {

  public static class CompactLevelEvent {
    private int level;

    /**
     * @return the level
     */
    public int getLevel() {
      return level;
    }

    /**
     * @param level the level to set
     */
    public void setLevel(int level) {
      this.level = level;
    }

    /**
     * @param level the level to set
     * @return 
     */
    public CompactLevelEvent withLevel(int level) {
      this.setLevel(level);
      return this;
    }
  }

  private SSTableManager ssTableManager;

  public CompactLevelEventListener(SSTableManager ssTableManager) {
    this.ssTableManager = ssTableManager;
  }

  @Subscribe
  public void compactLevel(CompactLevelEvent event) {
    ssTableManager.compactLevel(event.getLevel());
  }
}
