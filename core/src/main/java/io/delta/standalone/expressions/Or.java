package io.delta.standalone.expressions;

import java.util.Collection;

import io.delta.core.internal.expressions.ExpressionErrors;
import io.delta.standalone.types.BooleanType;

/**
 * Evaluates logical {@code expr1} OR {@code expr2} for {@code new Or(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans.
 */
public final class Or extends BinaryOperator implements Predicate {

    public static Or apply(Collection<Expression> disjunctions) {
        if (disjunctions.size() < 2) {
            throw new IllegalArgumentException("Or.apply must be called with at least 2 elements");
        }

        return (Or) disjunctions
            .stream()
            // we start off with Or(false, false)
            // then we get the 1st expression: Or(Or(false, false), expr1)
            // then we get the 2nd expression: Or(Or(false, false), expr1), expr2) etc.
            .reduce(new Or(Literal.False, Literal.False), Or::new);
    }

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
