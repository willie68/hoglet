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

/**
 * @author w.klaas
 *
 */
public class SSTIdentity {
  private int level;
  private int number;
  private int incarnation;

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

  /**
   * @return the reincarnation
   */
  public int getIncarnation() {
    return incarnation;
  }

  /**
   * @param incarnation the reincarnation to set
   */
  public void setIncarnation(int incarnation) {
    this.incarnation = incarnation;
  }

  /**
   * @param incarnation the reincarnation to set
   * @return 
   */
  public SSTIdentity withIncarnation(int incarnation) {
    setIncarnation(incarnation);
    return this;
  }

  @Override
  public String toString() {
    return String.format("level: %d, number: %d, incarnation: %s", level, number, incarnation);
  }
}
