package com.utils.record;

import com.utils.record.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

public class TestTuple2 {

  @Test
  public void testTuple2() {
    Tuple2<String, String> testTuple1 = new Tuple2<>("a", "b");
    Tuple2<String, String> testTuple2 = new Tuple2<>("a", "b");
    Tuple2<String, String> testTuple3 = new Tuple2<>("c", "d");

    Assert.assertThat(testTuple1.getForward(), is("a"));
    Assert.assertThat(testTuple1.getRear(), is("b"));
    Assert.assertThat(testTuple1.equals(testTuple1), is(true));
    Assert.assertThat(testTuple1 == testTuple2, is(false));
    Assert.assertThat(testTuple1.equals(testTuple2), is(true));
    Assert.assertThat(testTuple1.equals(testTuple3), is(false));
    Assert.assertThat(testTuple1.toString(), is("Tuple2(a, b)"));
    Assert.assertThat(testTuple3.toString(), is("Tuple2(c, d)"));
  }

  @Test
  public void test() {
  }
}
