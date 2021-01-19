package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.RelationMetadata;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

public class StreamRelation extends RelationMetadata {
    private Stream<Row> rows;

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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("StreamRelation{");
        sb.append("columns=").append(columns);
        sb.append(", rows=").append(rows);
        sb.append('}');
        return sb.toString();
    }


}
