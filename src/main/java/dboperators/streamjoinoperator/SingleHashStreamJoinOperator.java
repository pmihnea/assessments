package dboperators.streamjoinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.RelationMetadata;
import dboperators.Relations;
import dboperators.Row;
import de.infonautika.streamjoin.Join;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

/**
 Base on de.infonautika.streamjoin.Join implementation.<br/>
 It reads one relation completely and creates an index from it
 and then iterates through the second relation and creates the join result. <br/>
 Runtime complexity O(N1+N2*b) <br/>
 Memory complexity O(N2+N2/b)=O(N2) because it stores only one index in memory <br/>
*/
public class SingleHashStreamJoinOperator implements IStreamJoinOperator {
    private static final int CHUNK_SIZE = 1 << 10;

    public StreamRelation join(StreamRelation rel1, StreamRelation rel2) {
        Objects.requireNonNull(rel1);
        Objects.requireNonNull(rel2);

        final ArrayList<Column> commonColumns = Columns.intersection(rel1.getColumns(), rel2.getColumns());
        RelationMetadata commonRelationMetadata = new RelationMetadata(commonColumns);

        if (commonColumns.size() > 0) {
            // create the columns of the output relation
            ArrayList<Column> outRelationColumns = Columns.union(rel1.getColumns(), rel2.getColumns());
            RelationMetadata outRelationMetadata = new RelationMetadata(outRelationColumns);

            final Stream<Row> outRows = Join.join(rel1.getRows())
                    .withKey(row -> Relations.extractRow(rel1, row, commonRelationMetadata))
                    .on(rel2.getRows())
                    .withKey(row -> Relations.extractRow(rel2, row, commonRelationMetadata))
                    .combine((row1, row2) -> Relations.mergeRows(rel1, row1, rel2, row2, outRelationMetadata))
                    .asStream().parallel();

            return new StreamRelation(outRelationColumns,outRows);

        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }

}
