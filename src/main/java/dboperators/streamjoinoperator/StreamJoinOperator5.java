package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.RelationMetadata;
import dboperators.Relations;
import dboperators.Row;
import dboperators.joinoperator.Index;
import dboperators.joinoperator.Relation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * It uses the vavr library for sequential stream/iterator operations.<br>
 * Implements a custom join operation based on the following steps:
 */
public class StreamJoinOperator5 implements IStreamJoinOperator {

    @Override
    public StreamRelation join(StreamRelation rel1, StreamRelation rel2) {
        Objects.requireNonNull(rel1);
        Objects.requireNonNull(rel2);

        final ArrayList<Column> commonColumns = Columns.intersection(rel1.getColumns(), rel2.getColumns());

        if (commonColumns.size() > 0) {
            // create the columns of the output relation
            ArrayList<Column> outRelColumns = Columns.union(rel1.getColumns(), rel2.getColumns());
            RelationMetadata outRelMetadata = new RelationMetadata(outRelColumns);

            // create the stream of the output relation
            Iterator<Row> rowsIterator1 = rel1.getRows().iterator();
            Iterator<Row> rowsIterator2 = rel2.getRows().iterator();

            if (!rowsIterator1.hasNext() && !rowsIterator2.hasNext()) {
                // create the output relation
                return new StreamRelation(outRelColumns, Stream.empty());
            }else{
                // create empty indexes
                Index index1 = new Index(new Relation(rel1.getColumns()), commonColumns);
                Index index2 = new Index(new Relation(rel2.getColumns()), commonColumns);

                final Iterator<Row> resultIterator = new Iterator<>() {
                    final ArrayList<Row> EMPTY = new ArrayList<>(0);
                    ArrayList<Row> results = new ArrayList<>();
                    Iterator<Row> resultsIterator = results.iterator();
                    @Override
                    public boolean hasNext() {
                        while(!hasResults() && hasRows()) {
                            // read next rows
                            final Row row1 = rowsIterator1.hasNext() ? rowsIterator1.next() : null;
                            final Row row2 = rowsIterator2.hasNext() ? rowsIterator2.next() : null;

                            // join the rows with the old ones
                            // add row1 to index1 and join index1 with row2
                            if (row1 != null) {
                                index1.addIndexRow(row1);
                            }
                            final ArrayList<Row> resultRows1 = (row2 != null) ? index1.getValues().getOrDefault(Relations.extractRow(rel2, row2, index1), EMPTY) : EMPTY;
                            // join row1 with index2 without row2 to avoid the duplication of row1 with row2
                            final ArrayList<Row> resultRows2 = (row1 != null) ? index2.getValues().getOrDefault(Relations.extractRow(rel1, row1, index2), EMPTY) : EMPTY;
                            // add row2 to index2
                            if (row2 != null) {
                                index2.addIndexRow(row2);
                            }

                            // create results
                            results.clear();
                            results.ensureCapacity(resultRows1.size() + resultRows2.size());
                            resultRows1.forEach(r1 -> results.add(Relations.mergeRows(rel1, r1, rel2, row2, outRelMetadata)));
                            resultRows2.forEach(r2 -> results.add(Relations.mergeRows(rel2, r2, rel1, row1, outRelMetadata)));
                            resultsIterator = results.iterator();
                        }
                        return hasResults();
                    }

                    private boolean hasRows() {
                        return rowsIterator1.hasNext() || rowsIterator2.hasNext();
                    }

                    private boolean hasResults() {
                        return resultsIterator.hasNext();
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        return resultsIterator.next();
                    }
                };
                // create the output relation
                return new StreamRelation(outRelColumns, StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, 0), false));
            }
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }
}
