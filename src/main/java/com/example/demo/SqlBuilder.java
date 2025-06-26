public class SqlBuilder {
    private String tableName;
    private List<String> fields;
    private List<String> values;
    private List<String> whereConditions;
    private List<String> primaryKeys;
    private String operationType;
    
    // 构建INSERT SQL
    public String buildInsertSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        sql.append(String.join(", ", fields));
        sql.append(") VALUES (");
        sql.append(String.join(", ", values));
        sql.append(")");
        return sql.toString();
    }
    
    // 构建UPDATE SQL
    public String buildUpdateSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        
        List<String> setPairs = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (!primaryKeys.contains(fields.get(i))) {
                setPairs.add(fields.get(i) + " = " + values.get(i));
            }
        }
        
        sql.append(String.join(", ", setPairs));
        sql.append(" WHERE ");
        
        List<String> primaryConditions = new ArrayList<>();
        for (String pk : primaryKeys) {
            int idx = fields.indexOf(pk);
            if (idx >= 0) {
                primaryConditions.add(pk + " = " + values.get(idx));
            }
        }
        
        sql.append(String.join(" AND ", primaryConditions));
        return sql.toString();
    }
    
    // 构建UPSERT SQL (MySQL方式)
    public String buildUpsertSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        sql.append(String.join(", ", fields));
        sql.append(") VALUES (");
        sql.append(String.join(", ", values));
        sql.append(") ON DUPLICATE KEY UPDATE ");
        
        List<String> updatePairs = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (!primaryKeys.contains(fields.get(i))) {
                updatePairs.add(fields.get(i) + " = VALUES(" + fields.get(i) + ")");
            }
        }
        
        sql.append(String.join(", ", updatePairs));
        return sql.toString();
    }
    
    // 构建DELETE SQL
    public String buildDeleteSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName);
        
        if (whereConditions != null && !whereConditions.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(String.join(" AND ", whereConditions));
        }
        
        return sql.toString();
    }
    
    // getter和setter方法
    // ...
}
