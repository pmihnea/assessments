package joinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RelationsTest {
    @Test
    public void testMergeRows(){
        final Relation relation1 = SampleRelations.getRelation1();
        final Relation relation2 = SampleRelations.getRelation2();
        final Row row = Relations.mergeRows(relation1, relation1.getRows().get(0),
                relation2, relation2.getRows().get(0),
                SampleRelations.getRelation3());
        Assertions.assertArrayEquals(new Object[]{"x1", "y1", "z1"}, row.getValues());
    }
}
