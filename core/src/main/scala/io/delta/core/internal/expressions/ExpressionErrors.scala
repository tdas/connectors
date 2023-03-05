package io.delta.core.internal.expressions

import scala.annotation.varargs

object ExpressionErrors {
  @varargs def illegalExpressionValueType(
    exprName: String,
    expectedType: String,
    realTypes: String*): RuntimeException = {
    new IllegalArgumentException(
      s"$exprName expression requires $expectedType type. But found ${realTypes.mkString(", ")}");
  }
}
