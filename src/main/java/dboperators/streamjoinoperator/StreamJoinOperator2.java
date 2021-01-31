package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.Row;
import dboperators.joinoperator.Index;
import dboperators.joinoperator.JoinOperator;
import dboperators.joinoperator.Relation;
import one.util.streamex.StreamEx;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Based on StreamEx library and its cross product function (between a stream and a collection)
public class StreamJoinOperator2 implements IStreamJoinOperator {
    private static final int CHUNK_SIZE = 1 << 10;

    public StreamRelation join(StreamRelation rel1, StreamRelation rel2) {
        Objects.requireNonNull(rel1);
        Objects.requireNonNull(rel2);

        final ArrayList<Column> commonColumns = Columns.intersection(rel1.getColumns(), rel2.getColumns());

        if (commonColumns.size() > 0) {
            // create the columns of the output relation
            ArrayList<Column> outRelColumns = Columns.union(rel1.getColumns(), rel2.getColumns());

            // create the stream of the output relation
            final BiPredicate<Row, Row> sameGroup1 = new SameGroupPredicate();
            final StreamEx<Relation> relations1 = StreamEx.of(rel1.getRows()).groupRuns(sameGroup1)
                    .map(rows -> new Relation(rel1.getColumns(), rows));
            //final List<Relation> relations1List = relations1.collect(Collectors.toList());//TODO remove it

            final BiPredicate<Row, Row> sameGroup2 = new SameGroupPredicate();
            final StreamEx<Relation> relations2 = StreamEx.of(rel2.getRows()).groupRuns(sameGroup2)
                    .map(rows -> new Relation(rel2.getColumns(), rows));
            final StreamEx<Index> indexes2 = relations2.map(rc2 -> new Index(rc2, commonColumns));
            final List<Index> indexes2List = indexes2.collect(Collectors.toList());

            final Stream<Relation> outRelations = relations1.cross(indexes2List)
                    .map(entry -> JoinOperator.join(entry.getKey(), entry.getValue()));
            /*final Stream<Relation> outRelations = relations1List.stream().flatMap( relation1 -> indexes2List.stream()
                    .map(index2 -> new JoinOperator().join(relation1, index2)));*/

            // create the output relation
            return new StreamRelation(outRelColumns, outRelations.flatMap(outRel -> outRel.getRows().stream()).parallel());
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }

    private static class SameGroupPredicate implements BiPredicate<Row, Row> {
        int count = 1;

        @Override
        public boolean test(Row row, Row row2) {
            count++;
            if (count > CHUNK_SIZE) {
                count = 1;
                return false;
            }
            return true;
        }
    };
}
