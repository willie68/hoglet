package de.mcs.hoglet.sst;

public class SSTException extends Exception {

  public SSTException() {
    super();
  }

  public SSTException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public SSTException(String message, Throwable cause) {
    super(message, cause);
  }

  public SSTException(String message) {
    super(message);
  }

  public SSTException(Throwable cause) {
    super(cause);
  }

}
