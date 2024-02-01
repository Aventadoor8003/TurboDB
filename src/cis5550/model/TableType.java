package cis5550.model;

public enum TableType {
    MEMORY,
    PERSISTENT;
    public static TableType getType(String name) {
        return name.startsWith("pt-") ? PERSISTENT : MEMORY;
    }
    public static TableType getType(Table table) {
        return table instanceof PersistentTable ? PERSISTENT : MEMORY;
    }
}
