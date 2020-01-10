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
package de.mcs.hoglet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * @author wklaa_000
 *
 */
class TestOptions {

  @Test
  void test() {
    Options src = Options.defaultOptions().withPath("123456/123");
    String yaml = src.toYaml();
    Options dest = Options.fromYaml(yaml);
    assertEquals(src.getChunkSize(), dest.getChunkSize());
    assertEquals(src.getMemTableMaxKeys(), dest.getMemTableMaxKeys());
    assertEquals(src.getMemTableMaxSize(), dest.getMemTableMaxSize());
    assertEquals(src.getPath(), dest.getPath());
    assertEquals(src.getVlogPath(), dest.getVlogPath());
    assertEquals(src.getVLogAge(), dest.getVLogAge());
    assertEquals(src.getVlogMaxChunkCount(), dest.getVlogMaxChunkCount());
    assertEquals(src.getVlogMaxFileCount(), dest.getVlogMaxFileCount());
    assertEquals(src.getVlogMaxSize(), dest.getVlogMaxSize());
  }

}
