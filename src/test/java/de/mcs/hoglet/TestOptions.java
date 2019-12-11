/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: TestOptions.java
 * EMail: W.Klaas@gmx.de
 * Created: 10.12.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author wklaa_000
 *
 */
class TestOptions {

  @Test
  void test() {
    Options src = Options.defaultOptions().withPath("123456/123");
    String yaml = src.toYamlString();
    Options dest = Options.fromYamlString(yaml);
    assertEquals(src.getChunkSize(), dest.getChunkSize());
    assertEquals(src.getMemTableMaxKeys(), dest.getMemTableMaxKeys());
    assertEquals(src.getMemTableMaxSize(), dest.getMemTableMaxSize());
    assertEquals(src.getPath(), dest.getPath());
    assertEquals(src.getVLogAge(), dest.getVLogAge());
    assertEquals(src.getVlogMaxChunkCount(), dest.getVlogMaxChunkCount());
    assertEquals(src.getVlogMaxFileCount(), dest.getVlogMaxFileCount());
    assertEquals(src.getVlogMaxSize(), dest.getVlogMaxSize());
  }

}
