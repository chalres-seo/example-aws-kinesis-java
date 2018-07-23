package com.aws.credentials;

import org.junit.*;

import static org.hamcrest.CoreMatchers.is;

public class TestCredentialsFactory {

  @Test
  public void testAWSCredentialsFactory() {
    Assert.assertThat(CredentialsFactory.getInstance().getCredentialsProvider() != null, is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAWSCredentialsException() {
    Assert.assertThat(CredentialsFactory.getInstance().getCredentialsProvider("exceptionProfile") == null, is(true));
  }
}
