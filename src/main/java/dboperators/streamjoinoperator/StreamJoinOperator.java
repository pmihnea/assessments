package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

public class StreamJoinOperator {
    public StreamRelation join(StreamRelation r1, StreamRelation r2) {
        Objects.requireNonNull(r1);
        Objects.requireNonNull(r2);

        final ArrayList<Column> commonColumns = Columns.intersection(r1.getColumns(), r2.getColumns());

        if (commonColumns.size() > 0) {
            // create the columns of the output relation
            ArrayList<Column> outRelColumns = Columns.union(r1.getColumns(), r2.getColumns());

            // create the stream of the output relation
            Stream<Row> outRelRowStream = null;
            r1.getRows().colle

            // create the output relation
            return new StreamRelation(outRelColumns, outRelRowStream);
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }
}
