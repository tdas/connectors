package io.delta.core.internal.expressions;

import java.util.Comparator;

public class CastingComparator<T extends Comparable<T>> implements Comparator<Object> {
    private final Comparator<T> comparator;

    public CastingComparator() {
        comparator = Comparator.naturalOrder();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object a, Object b) {
        return comparator.compare((T) a, (T) b);
    }
}
