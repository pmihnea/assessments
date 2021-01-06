package joinoperator;

public class SampleRelations {
    public static Relation getRelation1(){
        final Relation r1 = Relation.of(SampleColumns.X, SampleColumns.Y);
        r1.getRows().add(Row.of("x1","y1"));
        r1.getRows().add(Row.of("x1","y2"));
        r1.getRows().add(Row.of("x2","y1"));
        r1.getRows().add(Row.of("x2","y2"));
        r1.getRows().add(Row.of("x3","y3"));
        return r1;
    }
    public static Relation getRelation2(){
        final Relation r2 = Relation.of(SampleColumns.X, SampleColumns.Z);
        r2.getRows().add(Row.of("x1","z1"));
        r2.getRows().add(Row.of("x2","z2"));
        return r2;
    }
    public static Relation getRelation3(){
        final Relation r3 = Relation.of(SampleColumns.X,SampleColumns.Y, SampleColumns.Z);
        return r3;
    }
}
