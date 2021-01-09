package joinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RelationTest {
    @Test
    public void testAddMergedRows(){
        final Relation relation1 = SampleRelations.getRelation1();
        final Relation relation2 = SampleRelations.getRelation2();
        final Relation relation3 = SampleRelations.getRelation3();
        final Row row = relation3.addMergedRow(relation1, relation1.getRows().get(0),
                relation2, relation2.getRows().get(0));
        Assertions.assertArrayEquals(new Object[]{"x1", "y1", "z1"}, row.getValues());
    }
}
