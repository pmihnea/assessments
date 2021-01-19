package dboperators.joinoperator;

import dboperators.Column;
import dboperators.ColumnType;

public interface SampleColumns {
    Column X = new Column("X", ColumnType.VARCHAR);
    Column Y = new Column("Y", ColumnType.VARCHAR);
    Column Z = new Column("Z", ColumnType.VARCHAR);
    Column P = new Column("P", ColumnType.VARCHAR);
    Column Q = new Column("Q", ColumnType.VARCHAR);
}
