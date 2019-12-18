/**
 * 
 */
package de.mcs.hoglet.sst;

import java.io.IOException;

import de.mcs.hoglet.Options;
import de.mcs.utils.MMFUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class SSTableReaderFactory {
  private static Logger log = Logger.getLogger(SSTableReaderFactory.class);

  public static SSTableReader getReader(Options options, int readingLevel, int number)
      throws SSTException, IOException {
    boolean capableOfUnmapMMF = MMFUtils.isSystemCapableOfUnmapMMF();
    if (capableOfUnmapMMF && Options.SSTReadStrategy.MMF.equals(options.getSstReadStrategy())) {
      return new SSTableReaderMMF(options, readingLevel, number);
    } else {
      if (!capableOfUnmapMMF) {
        log.warn("system is not capable of using mmf files.");
      }
      return new SSTableReaderRAF(options, readingLevel, number);
    }
  }

}
