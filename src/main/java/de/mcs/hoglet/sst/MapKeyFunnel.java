/**
 * 
 */
package de.mcs.hoglet.sst;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * @author w.klaas
 *
 */
public class MapKeyFunnel implements Funnel<MapKey> {
  private static final long serialVersionUID = 4637349648234815078L;

  @Override
  public void funnel(MapKey from, PrimitiveSink into) {
    into.putBytes(from.getKey());
  }

}
