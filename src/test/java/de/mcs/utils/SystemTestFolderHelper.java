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
package de.mcs.utils;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import de.mcs.jmeasurement.JMConfig;
import de.mcs.jmeasurement.MeasureFactory;

/**
 * @author wklaa_000
 *
 */
public class SystemTestFolderHelper {

  private static final String EASY_DB_FOLDER_PATH = "h:/temp/hogletdb/";
  private static final String DB_FOLDER_PATH = "e:/temp/hogletdb/";

  private static final String RAM_DISK_EASY_DB_FOLDER_PATH = "r:/temp/hogletdb/";
  private static final String RAM_DISK_DB_FOLDER_PATH = "r:/temp/hogletdb/";

  public static SystemTestFolderHelper newSystemTestFolderHelper() {
    return new SystemTestFolderHelper().withUseOnlyBaseFolder(false).withRAMDisk(true);
  }

  private boolean deleteBeforeTest;
  private boolean useBaseFolder;
  private boolean ramDisk;

  public SystemTestFolderHelper withDeleteBeforeTest(boolean deleteBeforeTest) {
    this.deleteBeforeTest = deleteBeforeTest;
    return this;
  }

  public SystemTestFolderHelper withUseOnlyBaseFolder(boolean useBaseFolder) {
    this.useBaseFolder = useBaseFolder;
    return this;
  }

  public File getFolder() throws IOException, InterruptedException {
    File baseFolder = new File(DB_FOLDER_PATH);
    if (ramDisk) {
      baseFolder = new File(RAM_DISK_DB_FOLDER_PATH);
      if ("CBP1ZF2".equals(SystemHelper.getComputerName())) {
        baseFolder = new File(RAM_DISK_EASY_DB_FOLDER_PATH);
      }
    } else {
      if ("CBP1ZF2".equals(SystemHelper.getComputerName())) {
        baseFolder = new File(EASY_DB_FOLDER_PATH);
      }
    }
    baseFolder.mkdirs();
    if (deleteBeforeTest) {
      de.mcs.utils.Files.remove(baseFolder, true);
      baseFolder.mkdirs();
    }
    File dbFolder = baseFolder;
    if (!useBaseFolder) {
      Random rnd = new Random(System.currentTimeMillis());
      while (dbFolder.exists()) {
        String foldername = String.format("test_%d", rnd.nextInt(100000));
        dbFolder = new File(baseFolder, foldername);
      }
    }
    dbFolder.mkdirs();
    System.out.println("Using folder: " + dbFolder.getAbsolutePath());
    return dbFolder;
  }

  public static void initStatistics() {
    MeasureFactory.setOption(JMConfig.OPTION_DISABLE_DEVIATION, "true");
  }

  public static void outputStatistics() {
    System.out.println();
    System.out.println(MeasureFactory.asString());
  }

  public SystemTestFolderHelper withRAMDisk(boolean ramDisk) {
    this.ramDisk = ramDisk;
    return this;
  }

}
