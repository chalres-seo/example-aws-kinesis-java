package com.apps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class AppMain {
  private static Logger logger = LoggerFactory.getLogger(AppMain.class);

  /**
   *
   * @param args
   *             0 : mode (consumer or producer)
   *             1 : profile name.
   *             2 : region name.
   *             3 : stream name.
   *             4 : example record count per 1sec. (only producer)
   */
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    logger.info("start example api main");

    if (args[0].equals("consumer")) {
      ExampleAPIKinesisConsume.main(args);
    } else {
      ExampleAPIKinesisProduce.main(args);
    }
  }
}