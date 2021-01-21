package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.RelationMetadata;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class StreamRelation extends RelationMetadata {
    private Stream<Row> rows;
    private Optional<Long> estimatedRowsCount = Optional.empty();

    public static StreamRelation of(Stream<Row> rows, Column... columns) {
        return new StreamRelation(new ArrayList<>(Arrays.asList(columns)), rows);
    }

    public StreamRelation(ArrayList<Column> columns, Stream<Row> rows) {
        super(columns);
        this.rows = rows;
    }

    public Stream<Row> getRows() {
        return rows;
    }

    public Optional<Long> getEstimatedRowsCount() {
        return estimatedRowsCount;
    }

    public void setEstimatedRowsCount(Optional<Long> estimatedRowsCount) {
        this.estimatedRowsCount = estimatedRowsCount;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StreamRelation.class.getSimpleName() + "[", "]")
                .add("columns=" + columns)
                .add("rows=" + rows)
                .add("estimatedRowsCount=" + estimatedRowsCount)
                .toString();
    }

}
