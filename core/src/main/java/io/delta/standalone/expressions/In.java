/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.expressions;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.core.internal.expressions.Util;
import io.delta.standalone.data.RowRecord;

/**
 * Evaluates if {@code expr} is in {@code exprList} for {@code new In(expr, exprList)}. True if
 * {@code expr} is equal to any expression in {@code exprList}, else false.
 */
public final class In implements Predicate {
    private final Expression value;
    private final List<? extends Expression> elems;
    private final Comparator<Object> comparator;

    /**
     * @param value  a nonnull expression
     * @param elems  a nonnull, nonempty list of expressions with the same data type as
     *               {@code value}
     */
    public In(Expression value, List<? extends Expression> elems) {
        if (null == value) {
            throw new IllegalArgumentException("'In' expression 'value' cannot be null");
        }
        if (null == elems) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be null");
        }
        if (elems.isEmpty()) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be empty");
        }

        boolean allSameDataType = elems
            .stream()
            .allMatch(x -> x.dataType().equivalent(value.dataType()));

        if (!allSameDataType) {
            throw new IllegalArgumentException(
                "In expression 'elems' and 'value' must all be of the same DataType");
        }

        this.value = value;
        this.elems = elems;
        this.comparator = Util.createComparator(value.dataType());
    }

    /**
     * This implements the {@code IN} expression functionality outlined by the Databricks SQL Null
     * semantics reference guide. The logic is as follows:
     * <ul>
     *     <li>TRUE if the non-NULL value is found in the list</li>
     *     <li>FALSE if the non-NULL value is not found in the list and the list does not contain
     *     NULL values</li>
     *     <li>NULL if the value is NULL, or the non-NULL value is not found in the list and the
     *     list contains at least one NULL value</li>
     * </ul>
     *
     * @see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#in-and-not-in-subqueries">NULL Semantics</a>
     */
    @Override
    public Boolean eval(RowRecord record) {
        Object origValue = value.eval(record);
        if (null == origValue) return null;

        // null if a null value has been found in list, otherwise false
        Boolean falseOrNullresult = false;
        for (Expression setElem : elems) {
            Object setElemValue = setElem.eval(record);
            if (setElemValue == null) {
                // null value found but element may still be in list
                falseOrNullresult = null;
            } else if (comparator.compare(origValue, setElemValue) == 0) {
                // short circuit and return true; we have found the element in the list
                return true;
            }

        }
        return falseOrNullresult;
    }

    @Override
    public String toString() {
        String elemsStr = elems
            .stream()
            .map(Expression::toString)
            .collect(Collectors.joining(", "));
        return value + " IN (" + elemsStr + ")";
    }

    @Override
    public List<Expression> children() {
        return Stream.concat(Stream.of(value), elems.stream()).collect(Collectors.toList());
    }
}
