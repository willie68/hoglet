/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: SystemTestFolderHelper.java
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
package de.mcs.utils;

import java.io.File;
import java.io.IOException;

import de.mcs.jmeasurement.JMConfig;
import de.mcs.jmeasurement.MeasureFactory;

/**
 * @author wklaa_000
 *
 */
public class SystemTestFolderHelper {

  private static final String DB_FOLDER_PATH = "e:/temp/hogletdb/";
  private static final String EASY_DB_FOLDER_PATH = "r:/temp/hogletdb/";

  public static File initFolder() throws IOException, InterruptedException {
    return initFolder(true);
  }

  public static File initFolder(boolean deleteBeforeTest) throws IOException, InterruptedException {
    File dbFolder = new File(DB_FOLDER_PATH);
    if ("CBP1ZF2".equals(SystemHelper.getComputerName())) {
      dbFolder = new File(EASY_DB_FOLDER_PATH);
    }
    if (dbFolder.exists() && deleteBeforeTest) {
      Files.remove(dbFolder, true);
      Thread.sleep(100);
    }
    dbFolder.mkdirs();
    return dbFolder;
  }

  public static void initStatistics() {
    MeasureFactory.setOption(JMConfig.OPTION_DISABLE_DEVIATION, "true");
  }

  public static void outputStatistics() {
    System.out.println();
    System.out.println(MeasureFactory.asString());
  }

}
