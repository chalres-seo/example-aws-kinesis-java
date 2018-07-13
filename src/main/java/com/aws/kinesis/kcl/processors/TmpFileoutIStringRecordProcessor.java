package com.aws.kinesis.kcl.processors;

import com.amazonaws.services.kinesis.model.Record;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TmpFileoutIStringRecordProcessor extends AbstractIStringRecordProcessor {
  private static final int NUM_RETRIES = 10;

  private static final Logger logger = LoggerFactory.getLogger(TmpFileoutIStringRecordProcessor.class);
  private static final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  @Override
  public void processRecordsWithRetries(List<Record> records) {
    logger.debug("process records");
    boolean processedSuccessfully = false;
    List<Record> unprocessedRecords = records;

    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        logger.debug("process records retry count : " + i);
        // Logic to process record goes here.
        unprocessedRecords = this.processFileWrite(unprocessedRecords);
        processedSuccessfully = unprocessedRecords.size() == 0;
        break;
      } catch (Throwable t) {
        logger.warn("Caught throwable while processing records", t);
      }
      // backoff if we encounter an exception.
      backoff();
    }
    if (!processedSuccessfully) {
      logger.error("Couldn't process record. Skipping the record.");
      logger.error("stopped process record : ");
    }
  }

  private List<Record> processFileWrite(List<Record> records) throws IOException {
    final Path tmpFilePath = Paths.get("tmp/" + DateTime.now().toString("yyyyMMdd-HHmm")
      + "-" + super.getShardId() + ".kinesis");

    logger.debug("Temp file path : " + tmpFilePath.getFileName());

    if (Files.notExists(tmpFilePath)) {
      Files.createFile(tmpFilePath);
    }

    final List<Record> failedRecords;

    try (final BufferedWriter writer = Files.newBufferedWriter(tmpFilePath,
      StandardCharsets.UTF_8,
      StandardOpenOption.APPEND)) {

      failedRecords = records.stream()
        .map(record -> this.processSingleRecordFileWrite(writer, record))
        .filter(Objects::nonNull).collect(Collectors.toList());

      writer.flush();

      return failedRecords;
    }
  }

  private Record processSingleRecordFileWrite(final BufferedWriter writer, final Record record) {
    try {
      final String stringRecord = decoder.decode(record.getData()).toString();
      logger.debug("Append write temp file, record : " + stringRecord);

      writer.write(stringRecord);
      writer.newLine();
      return null;
    } catch (CharacterCodingException e) {
      logger.debug("skipped decode failed record, sequence : " + record.getSequenceNumber());
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      logger.debug("IO exception record, put retry record list, sequence : " + record.getSequenceNumber());
      e.printStackTrace();
      return record;
    }
  }

  @Override
  public void processSingleRecord(Record record) {}
}
