package com.utils;

import com.aws.kinesis.record.IRecord;
import com.aws.kinesis.record.StringRecord;
import com.sun.istack.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AppUtils {
  private static final Logger logger = LoggerFactory.getLogger(AppUtils.class);

  private static long BACKOFF_TIME_IN_MILLIS = AppConfig.getRetryBackoffTimeInMillis();
  private static int RETRY_COUNT = AppConfig.getRetryAttemptCount();

  // Singleton >>
  private AppUtils() {}

  private static class LazyHolder {
    private static final AppUtils INSTANCE = new AppUtils();
  }

  public static AppUtils getInstance() {
    return AppUtils.LazyHolder.INSTANCE;
  }
  // << Singleton

  public static boolean checkDirAndIfNotExistCreate(final String filePathString) {
    logger.debug("check file path and create.");
    final Path createDirPath = Paths.get(filePathString).getParent();

    if (Files.notExists(createDirPath)) {
      logger.debug("dir is not exist. create: " + createDirPath);

      try {
        final Path createFileResultPath = Files.createDirectory(createDirPath);
        return createFileResultPath == createDirPath;
      } catch (IOException e) {
        logger.error("failed create dir. path: " + createDirPath);
        logger.error(e.getMessage());
        return false;
      }

    } else {
      return true;
    }
  }

  public static void backoff(@Nullable final String msg, final long backoffMillis) {
    try {
      logger.debug("backoff " + backoffMillis + " millis, " + msg);
      if (backoffMillis < 0) {
        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
      } else {
        Thread.sleep(backoffMillis);
      }
    } catch (InterruptedException e) {
      logger.error("backoff interrupted sleep", e);
      logger.error(e.getMessage());
    }
  }
  public static void backoff(final String msg) { backoff(msg, BACKOFF_TIME_IN_MILLIS); }

  public static void backoff(long backoffMillis) { backoff(null, backoffMillis); }

  public static void backoff() { backoff(BACKOFF_TIME_IN_MILLIS); }

  public static List<IRecord> createExampleRecords(final int recordCount) {
    logger.debug("create example records. count: " + recordCount);
    final ArrayList<IRecord> exampleRecords = new ArrayList<>(recordCount);
    for(int i = 1; i <= recordCount; i++) {
      try {
        final IRecord createRecord = new StringRecord("pk-" + i, "data-" + i);
//        logger.debug(String.format("create example record. record: StringRecord(pk-%s, data-%s)", i, i));
//        exampleRecords.add(new StringRecord("pk-" + i, "data-" + i));
        logger.debug("create example record. record: " + createRecord.toString());
        exampleRecords.add(createRecord);
      } catch (CharacterCodingException e) {
        logger.error(String.format("failed create example record. skipped record: StringRecord(pk-%s, data-%s)", i, i));
      }
    }

    return exampleRecords;
  }
}