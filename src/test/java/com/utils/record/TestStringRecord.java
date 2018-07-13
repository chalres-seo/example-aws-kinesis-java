package com.utils.record;

import com.aws.kinesis.record.StringRecord;
import com.utils.AppUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class TestStringRecord {
  final int EXAMPLE_RECORD_COUNT = 10;

  @Test
  public void testStringRecord() {
    final List<StringRecord> exampleRecords = AppUtils.getInstance().createExampleRecords(10);

    Assert.assertThat(exampleRecords.size(), is(10));
  }
}
