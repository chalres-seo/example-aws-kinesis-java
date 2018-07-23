package com.aws.kinesis.record.handler;

import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.IRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

public class TmpFileoutHandler<T> implements IRecordsHandler<T> {
  private final static Logger logger = LoggerFactory.getLogger(TmpFileoutHandler.class);

  private final static byte[] newLineBytes = "\n".getBytes();

  private final String tmpFilePathString;
  private final OpenOption[] openOptions;

  public TmpFileoutHandler(String tmpFilePathString, StandardOpenOption... openOptions) {
    this.tmpFilePathString = tmpFilePathString;
    this.openOptions = openOptions;
  }

  public TmpFileoutHandler(String tmpFilePathString) {
    this(tmpFilePathString, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
  }

  @Override
  public HandlerType getHandlerType() {
    return HandlerType.TmpFileoutHandler;
  }

  @Override
  public void recordsProcess(List<IRecord<T>> records) {
    logger.debug("process records. handler: " + getHandlerType() + ", count: " + records.size());

    try (final OutputStream outputStream = Files.newOutputStream(Paths.get(this.tmpFilePathString), this.openOptions)) {
      for (IRecord record : records) {
        logger.debug("write file to " + tmpFilePathString + ", record: " + record.toString());

        outputStream.write(record.getData().array());
        outputStream.write(newLineBytes);
      }
    } catch (IOException e) {
      logger.error("failed out put stream job. file path: " + tmpFilePathString +
        ", options: " + Arrays.toString(openOptions));
      logger.error(e.getMessage());
    }
  }

  @Override
  public void kinesisRecordsProcess(List<Record> kinesisRecords) {
    logger.debug("process kinesis records. handler: " + getHandlerType() + ", count: " + kinesisRecords.size());

    try (final OutputStream outputStream = Files.newOutputStream(Paths.get(this.tmpFilePathString), this.openOptions)) {
      for (Record kinesisRecord : kinesisRecords) {
        logger.debug("write file to " + tmpFilePathString + ", record: " + kinesisRecord.toString());

        outputStream.write(kinesisRecord.getData().array());
        outputStream.write(newLineBytes);
      }
    } catch (IOException e) {
      logger.error("failed out put stream job. file path: " + tmpFilePathString +
        ", options: " + Arrays.toString(openOptions));
      logger.error(e.getMessage());
    }
  }
}