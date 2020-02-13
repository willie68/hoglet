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
package de.mcs.hoglet.utils;

import java.io.File;
import java.io.FilenameFilter;

import com.google.common.io.Files;

import de.mcs.hoglet.Options;
import de.mcs.hoglet.sst.SSTIdentity;

/**
 * @author wklaa_000
 *
 */
public class DatabaseUtils {

  private static final String VLOG_FILENAME_FORMAT = "%08d.vlog";
  private static final String VLOG_FILENAME_REGEX_NUMBER = "(.*)\\.vlog";
  private static final String SST_FILENAME_REGEX_NUMBER = "sst_%02d_(.*)_%08h\\.sst";
  private static final String SST_FILENAME_FORMAT = "sst_%02d_%02d_%08h.sst";
  private static final String SST_INDEX_FILENAME_FORMAT = "sst_%02d_%02d_%08h.idx";

  public static DatabaseUtils newDatabaseUtils(Options options) {
    return new DatabaseUtils(options);
  }

  public static String getVLogFileName(int number) {
    return String.format(VLOG_FILENAME_FORMAT, number);
  }

  public static int getVLogFileNumber(String name) {
    String numberStr = Files.getNameWithoutExtension(name);
    return Integer.parseInt(numberStr);
  }

  public static File getVLogFilePath(File folder, int number) {
    return new File(folder, String.format(VLOG_FILENAME_FORMAT, number));
  }

  public static String getSSTFileName(SSTIdentity identity) {
    return String.format(SST_FILENAME_FORMAT, identity.getLevel(), identity.getNumber(), identity.getReincarnation());
  }

  public static File getSSTFilePath(File folder, SSTIdentity identity) {
    return new File(folder,
        String.format(SST_FILENAME_FORMAT, identity.getLevel(), identity.getNumber(), identity.getReincarnation()));
  }

  public static File getSSTIndexFilePath(File folder, SSTIdentity identity) {
    return new File(folder, String.format(SST_INDEX_FILENAME_FORMAT, identity.getLevel(), identity.getNumber(),
        identity.getReincarnation()));
  }

  private Options options;

  private DatabaseUtils(Options options) {
    this.options = options;
  }

  public int getSSTFileCount(final int level, final int reincarnation) {
    return getSSTFiles(level, reincarnation).length;
  }

  public File[] getSSTFiles(final int level, final int reincarnation) {
    File folder = new File(options.getPath());
    File[] listFiles = folder.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        String regex = String.format(SST_FILENAME_REGEX_NUMBER, level, reincarnation);
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
    File folder = new File(options.getVlogPath());
    File[] listFiles = folder.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.matches(VLOG_FILENAME_REGEX_NUMBER);
      }
    });
    return listFiles;
  }

  public File getSSTFilePath(SSTIdentity identity) {
    File dbFolder = new File(options.getPath());
    return getSSTFilePath(dbFolder, identity);
  }

  public File getSSTIndexFilePath(SSTIdentity identity) {
    File dbFolder = new File(options.getPath());
    return getSSTIndexFilePath(dbFolder, identity);
  }
}
