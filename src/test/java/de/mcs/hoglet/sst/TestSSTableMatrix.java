/**
 * MCS Media Computer Software
 * Copyright 2020 by Wilfried Klaas
 * Project: Hoglet
 * File: TestSSTableMatrix.java
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import de.mcs.hoglet.HogletDBException;

/**
 * @author wklaa_000
 *
 */
class TestSSTableMatrix {

  @Test
  void testIndicies() throws HogletDBException {
    SSTableMatrix build = SSTableMatrix.newTableMatrix().withLevels(10).withNumbers(10).build();
    assertNotNull(build);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity sstIdentity = build.getSSTIdentity(i, j);
        assertNull(sstIdentity);
      }
    }

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(0, -1);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(-1, 0);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(-1, -1);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(10, 0);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(0, 10);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      build.getSSTIdentity(10, 10);
    });
  }

  @Test
  void testAssertion() throws HogletDBException {
    SSTableMatrix build = SSTableMatrix.newTableMatrix().withLevels(10).withNumbers(10).build();
    assertNotNull(build);
    int incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(i).withNumber(j).withIncarnation(incarnation);
        build.setSSTIdentity(identity);
        incarnation++;
      }
    }
    incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity sstIdentity = build.getSSTIdentity(i, j);
        assertEquals(i, sstIdentity.getLevel());
        assertEquals(j, sstIdentity.getNumber());
        assertEquals(incarnation, sstIdentity.getIncarnation());
        incarnation++;
      }
    }

    final int incarnation2 = 10;
    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(0).withNumber(-1).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(-1).withNumber(0).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(-1).withNumber(-1).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(10).withNumber(0).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(0).withNumber(10).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(10).withNumber(10).withIncarnation(incarnation2);
      build.setSSTIdentity(identity);
    });
  }

  @Test
  void testRemove() throws HogletDBException {
    SSTableMatrix build = SSTableMatrix.newTableMatrix().withLevels(10).withNumbers(10).build();
    assertNotNull(build);
    int incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(i).withNumber(j).withIncarnation(incarnation);
        build.setSSTIdentity(identity);
        incarnation++;
      }
    }

    incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity sstIdentity = build.getSSTIdentity(i, j);
        assertEquals(i, sstIdentity.getLevel());
        assertEquals(j, sstIdentity.getNumber());
        assertEquals(incarnation, sstIdentity.getIncarnation());
        build.removeSSTIdentity(sstIdentity);
        incarnation++;
      }
    }

    incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity sstIdentity = build.getSSTIdentity(i, j);
        assertNull(sstIdentity);
        incarnation++;
      }
    }

    final int incarnation2 = 10;
    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(0).withNumber(-1).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(-1).withNumber(0).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(-1).withNumber(-1).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(10).withNumber(0).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(0).withNumber(10).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });

    Assertions.assertThrows(HogletDBException.class, () -> {
      SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(10).withNumber(10).withIncarnation(incarnation2);
      build.removeSSTIdentity(identity);
    });
  }

  @Test
  void testClone() throws HogletDBException, CloneNotSupportedException {
    SSTableMatrix build = SSTableMatrix.newTableMatrix().withLevels(10).withNumbers(10).build();
    assertNotNull(build);

    // prepare first incanation
    int incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(i).withNumber(j).withIncarnation(incarnation);
        build.setSSTIdentity(identity);
        incarnation++;
      }
    }

    // clone this one
    SSTableMatrix build2 = (SSTableMatrix) build.clone();

    // change incarnation
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity identity = SSTIdentity.newSSTIdentity().withLevel(i).withNumber(j).withIncarnation(incarnation);
        build.setSSTIdentity(identity);
        incarnation++;
      }
    }

    // test clone of first incanation
    incarnation = 0;
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        SSTIdentity sstIdentity = build2.getSSTIdentity(i, j);
        assertEquals(i, sstIdentity.getLevel());
        assertEquals(j, sstIdentity.getNumber());
        assertEquals(incarnation, sstIdentity.getIncarnation());
        incarnation++;
      }
    }
  }
}
