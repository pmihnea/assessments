package dboperators;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class Row {
    private Object[] values;

    public Row(Object[] values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    public static Row of(Object ... values){
        return new Row(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row row = (Row) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.deepEquals(values, row.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Row.class.getSimpleName() + "[", "]")
                .add("values=" + Arrays.toString(values))
                .toString();
    }
}
