package com.eakonovalov.storm.reconciliation.beans;

public enum ColumnType {

    KEY(0, "Key"),
    VALUE(1, "Value"),
    INCLUDE(2, "Include"),
    EXCLUDE(3, "Exclude"),
    SHOW(4, "Show");

    private final int code;
    private final String status;

    ColumnType(int code, String name) {
        this.code = code;
        this.status = name;
    }

    public static ColumnType get(String stringValue) {
        switch (stringValue) {
            case "Key":
                return KEY;
            case "Value":
                return VALUE;
            case "Include":
                return INCLUDE;
            case "Exclude":
                return EXCLUDE;
            case "Show":
                return SHOW;
        }

        return null;
    }

    public static ColumnType get(int intValue) {
        switch (intValue) {
            case 0:
                return KEY;
            case 1:
                return VALUE;
            case 2:
                return INCLUDE;
            case 3:
                return EXCLUDE;
            case 4:
                return SHOW;
        }

        return null;
    }

    public int getCode() {
        return code;
    }

    public String getType() {
        return status;
    }

}
