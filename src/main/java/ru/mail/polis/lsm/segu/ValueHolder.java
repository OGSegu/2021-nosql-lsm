package ru.mail.polis.lsm.segu;

import java.util.Objects;
import java.util.function.Supplier;

public class ValueHolder<T> {
    private T value = null;
    private Supplier<T> supplier;

    public ValueHolder(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T getValue() {
        if (value == null) {
            value = supplier.get();
        }
        return value;
    }

    @Override
    public String toString() {
        return "ValueHolder{" +
                "value=" + value +
                ", supplier=" + supplier +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueHolder<?> that = (ValueHolder<?>) o;
        return Objects.equals(value, that.value) && Objects.equals(supplier, that.supplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, supplier);
    }
}
