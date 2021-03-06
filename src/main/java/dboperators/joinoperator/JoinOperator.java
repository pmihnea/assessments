package dboperators.joinoperator;

import dboperators.Column;
import dboperators.Columns;
import dboperators.Relations;
import dboperators.Row;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

public class JoinOperator {
    /**
     * The overall join runtime complexity is <br>
     * O(n1) - building the index from one relation <br>
     * + <br>
     * O(n2) - for traversing the values of the second relation * O(1) for accessing the index * O(b) for iteration over one index bucket <br>
     * =  O(n1+b*n2) where b ~= n1/nIndex1 in average <br>
     * =  O(n1+n1*n2/nIndex1) = O(n1*(1+n2/nIndex1)) = O1 <br>
     * In case the second relation is indexed O(n2*(1+n1/nIndex2)) = O2 <br>
     * D = O1 - O2 can be written O( n1*n2*( NR - IR ) ) where NR = (n1-n2)/(n1*n2) and IR = (nIndex1-nIndex2)/(nIndex1*nIndex2) <br>
     * to see whether D > 0, we need to see whether NR > IR, which requires the creation of both indexes. <br>
     * The overall join memory complexity is O(n1) or O(n2) given by the index creation. <br>
     * In case we do not want to create both indexes, as it would increase the runtime complexity anyway,
     * we can rely mostly on the memory complexity.
     * So if n1 > n2 then we should index the second smaller relation with n2 elements as it takes less memory.
     */
    public static Relation join(Relation r1, Relation r2) {
        Objects.requireNonNull(r1);
        Objects.requireNonNull(r2);

        final ArrayList<Column> commonColumns = Columns.intersection(r1.getColumns(), r2.getColumns());

        if (commonColumns.size() > 0) {
            // perform a join

            // create an index of the smallest relation
            Relation sRel = r1.getRows().size() < r2.getRows().size() ? r1 : r2; //smallest
            Relation bRel = (sRel == r1) ? r2 : r1; //biggest
            final Index sRelIndex = new Index(sRel, commonColumns);

            return join(bRel, sRelIndex);
        } else {
            // create a Cartesian product as there is no common column
            //TODO: implement it or leave it as an error
            throw new IllegalStateException("The two input relations have no common column!");
        }
    }

    public static Relation join(Relation bRel, Index sRelIndex) {
        Objects.requireNonNull(bRel);
        Objects.requireNonNull(sRelIndex);
        Relation sRel = sRelIndex.getRelation();

        // create the columns of the output relation
        Relation outRel = new Relation(Columns.union(bRel.getColumns(), sRel.getColumns()));
        // create the values of the output relation
        for (Row bRow : bRel.getRows()) {
            final Row indexRow = Relations.extractRow(bRel, bRow, sRelIndex);
            final Optional<ArrayList<Row>> optSRows = Optional.ofNullable(sRelIndex.getValues().get(indexRow));
            optSRows.ifPresent(sRows -> sRows.forEach(sRow -> outRel.addMergedRow(bRel, bRow, sRel, sRow)));
        }
        return outRel;
    }

}
