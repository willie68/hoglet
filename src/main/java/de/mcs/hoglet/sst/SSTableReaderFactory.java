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
