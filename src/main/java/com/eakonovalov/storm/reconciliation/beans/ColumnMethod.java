package com.eakonovalov.storm.reconciliation.beans;

public enum ColumnMethod {

    NUMERIC(0, "Numeric"),
    TEXT(1, "Text"),
    IGNORECASE(2, "Ignore case"),
    DATE(3, "Date");

    private final int code;
    private final String name;

    ColumnMethod(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ColumnMethod get(String stringValue) {
        switch (stringValue) {
            case "Numeric":
                return NUMERIC;
            case "Text":
                return TEXT;
            case "Date":
                return DATE;
            case "Ignore case":
                return IGNORECASE;
        }

        return null;
    }

    public static ColumnMethod get(int intValue) {
        switch (intValue) {
            case 0:
                return NUMERIC;
            case 1:
                return TEXT;
            case 2:
                return DATE;
            case 3:
                return IGNORECASE;
        }

        return null;
    }

    public int getCode() {
        return code;
    }

    public String getMethod() {
        return name;
    }

}
