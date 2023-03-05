package io.delta.standalone.expressions;

import io.delta.core.internal.expressions.ExpressionErrors;
import io.delta.standalone.types.BooleanType;

/**
 * Evaluates logical {@code expr1} OR {@code expr2} for {@code new Or(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans.
 */
public final class Or extends BinaryOperator implements Predicate {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
        if (!(left.dataType() instanceof BooleanType) ||
                !(right.dataType() instanceof BooleanType)) {
            throw ExpressionErrors.illegalExpressionValueType(
                    "OR",
                    "bool",
                    left.dataType().getTypeName(),
                    right.dataType().getTypeName());
        }
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return (boolean) leftResult || (boolean) rightResult;
    }
}
