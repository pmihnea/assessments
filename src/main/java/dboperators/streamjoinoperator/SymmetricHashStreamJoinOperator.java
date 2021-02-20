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
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implements a stream join operation by creating two indexes while reading the streams rows and joining iteratively the two new rows with the old ones from the indexes. <br/>
 * The runtime complexity is O(N1*b2+N2*b1), where N1,N2 are number of rows of the two streams and b1,b2 are the sizes of the index buckets. <br/>
 * The memory complexity is O(N1+N1/b1+N2+N2/b2)=O(N1+N2) because it stores in memory both indexes. <br/>
 * */
public class SymmetricHashStreamJoinOperator implements IStreamJoinOperator {

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
                            // Read next rows
                            final Optional<Row> row1 = rowsIterator1.hasNext() ? Optional.of(rowsIterator1.next()) : Optional.empty();
                            final Optional<Row> row2 = rowsIterator2.hasNext() ? Optional.of(rowsIterator2.next()) : Optional.empty();

                            // Join the rows with the old ones
                            // add row1 to index1
                            row1.ifPresent(index1::addIndexRow);
                            // join index1 with row2
                            final ArrayList<Row> resultRows1 = row2.map(r2 -> index1.getValues().getOrDefault(Relations.extractRow(rel2, r2, index1), EMPTY)).orElse(EMPTY);
                            // join row1 with index2 without row2 to avoid the duplication of joining row1 with row2
                            final ArrayList<Row> resultRows2 = row1.map(r1 -> index2.getValues().getOrDefault(Relations.extractRow(rel1, r1, index2), EMPTY)).orElse(EMPTY);
                            // add row2 to index2
                            row2.ifPresent(index2::addIndexRow);

                            // Create results
                            results.clear();
                            results.ensureCapacity(resultRows1.size() + resultRows2.size());
                            row2.ifPresent(r2 -> resultRows1.forEach(r1 -> results.add(Relations.mergeRows(rel1, r1, rel2, r2, outRelMetadata))));
                            row1.ifPresent(r1 -> resultRows2.forEach(r2 -> results.add(Relations.mergeRows(rel2, r2, rel1, r1, outRelMetadata))));
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
