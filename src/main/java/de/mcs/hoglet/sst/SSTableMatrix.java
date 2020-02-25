/**
 * MCS Media Computer Software
 * Copyright 2020 by Wilfried Klaas
 * Project: Hoglet
 * File: SSTableMatrix.java
 * EMail: W.Klaas@gmx.de
 * Created: 25.02.2020 wklaa_000
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
package de.mcs.hoglet.sst;

import java.util.concurrent.locks.ReentrantLock;

import de.mcs.hoglet.HogletDBException;

/**
 * @author wklaa_000
 *
 */
public class SSTableMatrix implements Cloneable {

  static class SSTableMatrixBuilder {
    private int levelCount;
    private int numberCount;

    public SSTableMatrixBuilder withLevels(int levelCount) {
      this.levelCount = levelCount;
      return this;
    }

    public SSTableMatrixBuilder withNumbers(int numberCount) {
      this.numberCount = numberCount;
      return this;
    }

    public SSTableMatrix build() {
      return new SSTableMatrix().withLevels(levelCount).withNumbers(numberCount).build();
    }
  }

  private int[][] incarnationMatrix;
  private int levelCount;
  private int numberCount;
  private ReentrantLock lock = new ReentrantLock();

  public static SSTableMatrixBuilder newTableMatrix() {
    return new SSTableMatrixBuilder();
  }

  private SSTableMatrix() {

  }

  private SSTableMatrix withLevels(int levelCount) {
    this.levelCount = levelCount;
    return this;
  }

  private SSTableMatrix withNumbers(int numberCount) {
    this.numberCount = numberCount;
    return this;
  }

  private SSTableMatrix build() {
    incarnationMatrix = new int[levelCount][numberCount];
    for (int i = 0; i < incarnationMatrix.length; i++) {
      for (int j = 0; j < incarnationMatrix[i].length; j++) {
        incarnationMatrix[i][j] = -1;
      }
    }
    return this;
  }

  public SSTIdentity getSSTIdentity(int level, int number) throws HogletDBException {
    try {
      int incarnation = incarnationMatrix[level][number];
      if (incarnation == -1) {
        return null;
      }
      return SSTIdentity.newSSTIdentity().withLevel(level).withNumber(number).withIncarnation(incarnation);
    } catch (Exception e) {
      throw new HogletDBException(e);
    }
  }

  public void setSSTIdentity(SSTIdentity identity) throws HogletDBException {
    lock.lock();
    try {
      incarnationMatrix[identity.getLevel()][identity.getNumber()] = identity.getIncarnation();
    } catch (Exception e) {
      throw new HogletDBException(e);
    } finally {
      lock.unlock();
    }
  }

  public void removeSSTIdentity(SSTIdentity identity) throws HogletDBException {
    try {
      incarnationMatrix[identity.getLevel()][identity.getNumber()] = -1;
    } catch (Exception e) {
      throw new HogletDBException(e);
    }
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    SSTableMatrix clone = newTableMatrix().withLevels(levelCount).withNumbers(numberCount).build();
    lock.lock();
    try {
      for (int i = 0; i < incarnationMatrix.length; i++) {
        for (int j = 0; j < incarnationMatrix[i].length; j++) {
          clone.incarnationMatrix[i][j] = this.incarnationMatrix[i][j];
        }
      }
    } finally {
      lock.unlock();
    }
    return clone;
  }
}
