package de.mcs.hoglet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import de.mcs.hoglet.sst.SSTException;
import de.mcs.hoglet.utils.SSTExporter;

class TestSSTExporter {

  public static void main(String[] args) {
    File dbFolder = new File(args[0]);
    File exportFile = new File(dbFolder, "sst_export.txt");

    Options options = Options.defaultOptions().withPath(dbFolder.getAbsolutePath());
    try {
      SSTExporter exporter = new SSTExporter(options);
      try (OutputStream out = new FileOutputStream(exportFile)) {
        exporter.export(out);
      }
    } catch (SSTException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HogletDBException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
