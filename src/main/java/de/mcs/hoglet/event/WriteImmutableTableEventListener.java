/**
 * Copyright 2019 w.klaas
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
