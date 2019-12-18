package de.mcs.hoglet.sst;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import de.mcs.hoglet.Options;
import de.mcs.utils.SystemTestFolderHelper;

class TestSSTReaderFactory {

  private File dbFolder;
  private Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    SystemTestFolderHelper.initStatistics();
    dbFolder = SystemTestFolderHelper.initFolder(true);
    options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterEach
  void tearDown() throws Exception {
  }

  @AfterAll
  static void afterAll() {
    SystemTestFolderHelper.outputStatistics();
  }

  @Test
  void test() throws IOException, SSTException {
    assertThrows(SSTException.class, new Executable() {
      @Override
      public void execute() throws Throwable {
        try (SSTableReader reader = SSTableReaderFactory.getReader(options, 0, 0)) {
        }
      }
    });
  }

}
