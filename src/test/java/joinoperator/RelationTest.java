package joinoperator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RelationTest {
    @Test
    public void testAddMergedRows(){
        final Relation relation1 = SampleRelations.getRelationXY();
        final Relation relation2 = SampleRelations.getRelationXZ();
        final Relation relation3 = SampleRelations.getEmptyRelationXYZ();
        final Row row = relation3.addMergedRow(relation1, relation1.getRows().get(0),
                relation2, relation2.getRows().get(0));
        Assertions.assertArrayEquals(new Object[]{"x1", "y1", "z1"}, row.getValues());
    }
}
