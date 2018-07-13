package com.aws.kinesis.record;

import com.amazonaws.services.kinesis.model.Record;
import com.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Objects;
import java.util.Optional;

/**
 * @throws CharacterCodingException
 */
public class StringRecord implements IRecord<String> {
  private static final Logger logger = LoggerFactory.getLogger(StringRecord.class);

  private final String partitionKey;
  private final ByteBuffer readOnlyBytebuffer;
  private final String sequenceNumber;
  private final String data;

  public StringRecord(String partitionKey, ByteBuffer bytebuffer, String sequenceNumber, String data) {
    this.partitionKey = partitionKey;
    this.readOnlyBytebuffer = AppUtils.copyNewReadOnlyByteBuffer(bytebuffer);
    this.sequenceNumber = sequenceNumber;
    this.data = data;
  }

  public StringRecord(String partitionKey, ByteBuffer bytebuffer, String sequenceNumber) throws CharacterCodingException {
    this(partitionKey,
      bytebuffer,
      sequenceNumber,
      AppUtils.byteBufferToString(bytebuffer));
  }

  public StringRecord(String partitionKey, ByteBuffer bytebuffer) throws CharacterCodingException {
    this(partitionKey,
      bytebuffer,
      null,
      AppUtils.byteBufferToString(bytebuffer));
  }

  public StringRecord(String partitionKey, String sequenceNumber, String data) throws CharacterCodingException {
    this(partitionKey,
      AppUtils.stringToByteBuffer(data),
      sequenceNumber,
      data);
  }

  public StringRecord(String partitionKey, String data) throws CharacterCodingException {
    this(partitionKey, (String) null, data);
  }

  public StringRecord(Record record) throws CharacterCodingException {
    ByteBuffer copyReadOnlyByteByffer = record.getData().asReadOnlyBuffer();
    copyReadOnlyByteByffer.rewind();

    this.partitionKey = record.getPartitionKey();
    this.readOnlyBytebuffer = copyReadOnlyByteByffer;
    this.sequenceNumber = record.getSequenceNumber();
    this.data = AppUtils.byteBufferToString(copyReadOnlyByteByffer);
  }

  @Override
  public String getPartitionKey() {
    return this.partitionKey;
  }

  @Override
  public String getData() {
    return this.data;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    this.readOnlyBytebuffer.rewind();
    return this.readOnlyBytebuffer;
  }

  @Override
  public Optional<String> getSequenceNumber() {
    return Optional.ofNullable(sequenceNumber);
  }

  @Override
  public Record getKinesisRecord() {
    readOnlyBytebuffer.rewind();

    if (sequenceNumber == null) {
      return new Record()
        .withPartitionKey(this.partitionKey)
        .withData(this.readOnlyBytebuffer);
    } else {
      return new Record()
        .withPartitionKey(this.partitionKey)
        .withData(this.readOnlyBytebuffer)
        .withSequenceNumber(this.sequenceNumber);
    }
  }

  @Override
  public String toString() {
    return "StringRecord{" +
      "partitionKey='" + partitionKey + '\'' +
      ", readOnlyBytebuffer=" + readOnlyBytebuffer +
      ", sequenceNumber='" + sequenceNumber + '\'' +
      ", data='" + data + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringRecord that = (StringRecord) o;
    return Objects.equals(partitionKey, that.partitionKey) &&
      Objects.equals(readOnlyBytebuffer, that.readOnlyBytebuffer) &&
      Objects.equals(sequenceNumber, that.sequenceNumber) &&
      Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {

    return Objects.hash(partitionKey, readOnlyBytebuffer, sequenceNumber, data);
  }
}