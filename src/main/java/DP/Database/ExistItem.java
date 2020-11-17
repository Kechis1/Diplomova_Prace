package DP.Database;

public class ExistItem {
    DatabaseTable table;
    boolean not;

    public ExistItem(DatabaseTable table, boolean not) {
        this.table = table;
        this.not = not;
    }

    public DatabaseTable getTable() {
        return table;
    }

    public void setTable(DatabaseTable table) {
        this.table = table;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }
}
