package com.utils;

import com.aws.kinesis.record.StringRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AppUtils {
  private static final Logger logger = LoggerFactory.getLogger(AppUtils.class);

  private static final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
  private static final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  private AppUtils() {}

  private static class LazyHolder {
    private static final AppUtils INSTANCE = new AppUtils();
  }

  public static AppUtils getInstance() {
    return AppUtils.LazyHolder.INSTANCE;
  }

  public static boolean checkDirAndIfNotExistCreate(String filePathString) {
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

  /**
   *
   * @param string
   * @return
   * @throws CharacterCodingException
   */
  public static ByteBuffer stringToByteBuffer(String string) throws CharacterCodingException {
    return encoder.encode(CharBuffer.wrap(string)).asReadOnlyBuffer();
  }

  /**
   *
   * @param byteBuffer
   * @return
   * @throws CharacterCodingException
   */
  public static String byteBufferToString(ByteBuffer byteBuffer) throws CharacterCodingException {
    final ByteBuffer readOnlyByteBuffer = byteBuffer.asReadOnlyBuffer();

    readOnlyByteBuffer.rewind();
    final String decodeString = decoder.decode(readOnlyByteBuffer).toString();
    readOnlyByteBuffer.rewind();

    return decodeString;
  }

  public static ByteBuffer copyNewReadOnlyByteBuffer(ByteBuffer byteBuffer) {
    final ByteBuffer copy = byteBuffer.asReadOnlyBuffer();
    copy.rewind();

    return copy;
  }

  public static List<StringRecord> createExampleRecords(int recordCount) {
    logger.debug("create example records. count: " + recordCount);
    final ArrayList<StringRecord> exampleRecords = new ArrayList<>(recordCount);
    for(int i = 1; i <= recordCount; i++) {
      try {
        final StringRecord createRecord = new StringRecord("pk-" + i, "data-" + i);
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
