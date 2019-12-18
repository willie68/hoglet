/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: Hoglet
 * File: DatabaseUtils.java
 * EMail: W.Klaas@gmx.de
 * Created: 17.12.2019 wklaa_000
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
package de.mcs.hoglet.utils;

import java.io.File;
import java.io.FilenameFilter;

import de.mcs.hoglet.Options;

/**
 * @author wklaa_000
 *
 */
public class DatabaseUtils {

  private static final String VLOG_FILENAME_FORMAT = "vlog_%04d.vlog";
  private static final String VLOG_FILENAME_REGEX_NUMBER = "vlog_(.*)\\.vlog";
  private static final String SST_FILENAME_REGEX_NUMBER = "sst_%02d_(.*)\\.sst";
  private static final String SST_FILENAME_FORMAT = "sst_%02d_%02d.sst";

  public static DatabaseUtils newDatabaseUtils(Options options) {
    return new DatabaseUtils(options);
  }

  public static String getVLogFileName(int number) {
    return String.format(VLOG_FILENAME_FORMAT, number);
  }

  public static File getVLogFilePath(File folder, int number) {
    return new File(folder, String.format(VLOG_FILENAME_FORMAT, number));
  }

  public static String getSSTFileName(int level, int number) {
    return String.format(SST_FILENAME_FORMAT, level, number);
  }

  public static File getSSTFilePath(File folder, int level, int number) {
    return new File(folder, String.format(SST_FILENAME_FORMAT, level, number));
  }

  private Options options;

  private DatabaseUtils(Options options) {
    this.options = options;
  }

  public int getSSTFileCount(final int level) {
    return getSSTFiles(level).length;
  }

  public File[] getSSTFiles(final int level) {
    File folder = new File(options.getPath());
    File[] listFiles = folder.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        String regex = String.format(SST_FILENAME_REGEX_NUMBER, level);
        return name.matches(regex);
      }
    });
    return listFiles;
  }

  public int getVLogFileCount() {
    File[] vLogFiles = getVLogFiles();
    if (vLogFiles == null) {
      return 0;
    }
    return vLogFiles.length;
  }

  public File[] getVLogFiles() {
    File folder = new File(options.getPath());
    File[] listFiles = folder.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.matches(VLOG_FILENAME_REGEX_NUMBER);
      }
    });
    return listFiles;
  }
}
