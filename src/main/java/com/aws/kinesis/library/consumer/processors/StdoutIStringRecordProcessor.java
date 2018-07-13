package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class StdoutIStringRecordProcessor extends AbstractIStringRecordProcessor {
  private static final Logger logger = LoggerFactory.getLogger(StdoutIStringRecordProcessor.class);
  private static final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  @Override
  public void processSingleRecord(Record record) {
    String data = null;

    try {
      data = decoder.decode(record.getData()).toString();
      logger.info(String.format("sequence : %s, partition : %s, data : %s", record.getSequenceNumber(), record.getPartitionKey(), data));
      System.out.println("record : " + data);
    } catch (NumberFormatException e) {
      logger.info("Record does not match sample record format. Ignoring record with data; " + data);
    } catch (CharacterCodingException e) {
      logger.error("Malformed data: " + record.toString(), e);
    }
  }
}
