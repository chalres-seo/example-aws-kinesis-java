package com.aws.kinesis.record;

import com.amazonaws.services.kinesis.model.Record;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;

public class TestStringRecord {
  final private CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
  final private CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

  final private String testPartitionKey = "pk-1";
  final private String testValue = "data-1";
  final private String testSequenceNumber = "seq-1";
  final private ByteBuffer testData = encoder.encode(CharBuffer.wrap(testValue));

  final private Record testKinesisRecord = new Record()
    .withPartitionKey(testPartitionKey)
    .withData(testData)
    .withSequenceNumber(testSequenceNumber);

  public TestStringRecord() throws CharacterCodingException {}

  @Test
  public void testStringRecord() throws CharacterCodingException {
    Assert.assertThat(testKinesisRecord.getPartitionKey().equals(testPartitionKey), is(true));
    Assert.assertThat(testKinesisRecord.getData().equals(testData), is(true));
    Assert.assertThat(testKinesisRecord.getData() == testData, is(true));
    Assert.assertThat(testKinesisRecord.getSequenceNumber().equals(testSequenceNumber), is(true));

    final StringRecord stringRecord = new StringRecord(testPartitionKey, testData, testSequenceNumber);
    Assert.assertThat(stringRecord.getPartitionKey().equals(testPartitionKey), is(true));
    Assert.assertThat(stringRecord.getData() == testData, is(true));
    Assert.assertThat(stringRecord.getData().equals(testData), is(true));
    Assert.assertThat(stringRecord.getValue().equals(testValue), is(true));


    final StringRecord kinesisRecordToStringRecord = new StringRecord(testKinesisRecord);
    Assert.assertThat(kinesisRecordToStringRecord.getPartitionKey().equals(testPartitionKey), is(true));
    Assert.assertThat(kinesisRecordToStringRecord.getData() == testData, is(true));
    Assert.assertThat(kinesisRecordToStringRecord.getData().equals(testData), is(true));
    Assert.assertThat(kinesisRecordToStringRecord.getValue().equals(testValue), is(true));

    Assert.assertThat(stringRecord.equals(kinesisRecordToStringRecord), is(true));
    Assert.assertThat(stringRecord == kinesisRecordToStringRecord, is(false));
  }
}
