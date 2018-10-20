package com.eakonovalov.storm.reconciliation;

public class Query {

    public static final String GET_FILES = "select id, name, fullFileName as 'fileName', isPreFile, columnSetId, columnMapId, valueMapId from tbl_csv_file where id in (${IDs});";

    public static final String GET_COLUMN_SET = "select c.*, c.id, c.no, c.name, c.type, c.method from columnset s join rxcolumn c on s.id = c.columnSetId where s.id = ? order by c.no;";

    public static final String GET_COLUMN_MAP = "select c.id, c.no, c.name, c.mapTo, c.delta, c.tolerance from columnMap m join columnmapping c on m.id = c.columnMapId where m.id = ? order by c.no;";

}
