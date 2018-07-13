package com.utils;

import java.util.Objects;

public class Tuple2<A, B> {
  private final A forward;
  private final B rear;

  public Tuple2(A forward, B rear) {
    this.forward = forward;
    this.rear = rear;
  }

  public A getForward() {
    return forward;
  }

  public B getRear() {
    return rear;
  }

  @Override
  public String toString() {
    return "Tuple2("+ forward + ", " + rear +')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
    return Objects.equals(forward, tuple2.forward) &&
      Objects.equals(rear, tuple2.rear);
  }

  @Override
  public int hashCode() {
    return Objects.hash(forward, rear);
  }
}
