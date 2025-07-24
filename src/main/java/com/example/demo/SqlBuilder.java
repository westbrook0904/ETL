package com.dataloader.sql;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SQL构建器 - 支持单条和批量操作
 */
public class SqlBuilder {
    
    public enum OperationType { INSERT, UPDATE, UPSERT, DELETE }
    
    // 核心属性
    private String tableName;
    private List<String> fields = new ArrayList<>();
    private List<String> values = new ArrayList<>();
    private List<String> whereConditions = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();
    private OperationType operationType;
    
    // 构造函数
    public SqlBuilder() {}
    
    public SqlBuilder(String tableName) {
        this.tableName = tableName;
    }
    
    public SqlBuilder(String tableName, String operationType) {
        this.tableName = tableName;
        this.operationType = OperationType.valueOf(operationType.toUpperCase());
    }
    
    // ==================== 链式调用方法 ====================
    public SqlBuilder table(String tableName) { 
        this.tableName = tableName; 
        return this; 
    }
    
    public SqlBuilder fields(List<String> fields) { 
        this.fields = new ArrayList<>(fields); 
        return this; 
    }
    
    public SqlBuilder values(List<String> values) { 
        this.values = new ArrayList<>(values); 
        return this; 
    }
    
    public SqlBuilder where(List<String> whereConditions) { 
        this.whereConditions = new ArrayList<>(whereConditions); 
        return this; 
    }
    
    public SqlBuilder primaryKeys(List<String> primaryKeys) { 
        this.primaryKeys = new ArrayList<>(primaryKeys); 
        return this; 
    }
    
    public SqlBuilder operation(OperationType operationType) { 
        this.operationType = operationType; 
        return this; 
    }
    
    // ==================== Getter/Setter方法 ====================
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    
    public List<String> getFields() { return new ArrayList<>(fields); }
    public void setFields(List<String> fields) { this.fields = new ArrayList<>(fields); }
    
    public List<String> getValues() { return new ArrayList<>(values); }
    public void setValues(List<String> values) { this.values = new ArrayList<>(values); }
    
    public List<String> getWhereConditions() { return new ArrayList<>(whereConditions); }
    public void setWhereConditions(List<String> whereConditions) { this.whereConditions = new ArrayList<>(whereConditions); }
    
    public List<String> getPrimaryKeys() { return new ArrayList<>(primaryKeys); }
    public void setPrimaryKeys(List<String> primaryKeys) { this.primaryKeys = new ArrayList<>(primaryKeys); }
    
    public OperationType getOperationType() { return operationType; }
    public void setOperationType(OperationType operationType) { this.operationType = operationType; }
    
    // ==================== 校验方法 ====================
    private void validateTableName() {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }
    
    private void validateFieldsAndValues() {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Fields cannot be empty");
        }
        if (values != null && !values.isEmpty() && fields.size() != values.size()) {
            throw new IllegalArgumentException("Fields and values size mismatch");
        }
    }
    
    private void validateWhereConditions() {
        if (whereConditions == null || whereConditions.isEmpty()) {
            throw new IllegalArgumentException("Where conditions are required");
        }
    }
    
    private void validatePrimaryKeys() {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("Primary keys cannot be empty");
        }
    }
    
    // ==================== 工具方法 ====================
    private String joinFields(List<String> list) {
        return String.join(", ", list);
    }
    
    // ==================== 单条SQL构建方法 ====================
    public String buildSql() {
        validateTableName();
        if (operationType == null) {
            throw new IllegalArgumentException("Operation type is null");
        }
        
        switch (operationType) {
            case INSERT: return buildInsertSql();
            case UPDATE: return buildUpdateSql();
            case UPSERT: return buildUpsertSql();
            case DELETE: return buildDeleteSql();
            default: throw new IllegalArgumentException("Unsupported operation type: " + operationType);
        }
    }
    
    private String buildInsertSql() {
        validateFieldsAndValues();
        return "INSERT INTO " + tableName + " (" + joinFields(fields) + ") VALUES (" + joinFields(values) + ")";
    }
    
    private String buildUpdateSql() {
        validateFieldsAndValues();
        validateWhereConditions();
        
        List<String> setClause = new ArrayList<>();
        for (int i = 0; i < Math.min(fields.size(), values.size()); i++) {
            setClause.add(fields.get(i) + " = " + values.get(i));
        }
        
        return "UPDATE " + tableName + " SET " + String.join(", ", setClause) +
                " WHERE " + String.join(" AND ", whereConditions);
    }
    
    private String buildUpsertSql() {
        validateFieldsAndValues();
        if (primaryKeys == null) primaryKeys = new ArrayList<>();
        
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName)
           .append(" (").append(joinFields(fields)).append(")")
           .append(" VALUES (").append(joinFields(values)).append(")")
           .append(" ON DUPLICATE KEY UPDATE ");
           
        List<String> updateClause = new ArrayList<>();
        for (String field : fields) {
            if (!primaryKeys.contains(field)) {
                updateClause.add(field + " = VALUES(" + field + ")");
            }
        }
        sql.append(String.join(", ", updateClause));
        return sql.toString();
    }
    
    private String buildDeleteSql() {
        validateTableName();
        validateWhereConditions();
        return "DELETE FROM " + tableName + " WHERE " + String.join(" AND ", whereConditions);
    }
    
    // ==================== 批量SQL构建方法 ====================
    public String buildBatchInsertSql(int batchSize) {
        validateTableName();
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Fields cannot be empty for batch INSERT");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName)
           .append(" (").append(joinFields(fields)).append(")")
           .append(" VALUES ");
           
        List<String> valueGroups = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            List<String> batchValues = new ArrayList<>();
            for (String field : fields) {
                batchValues.add("#{paramsList[" + i + "]." + field + "}");
            }
            valueGroups.add("(" + String.join(", ", batchValues) + ")");
        }
        sql.append(String.join(", ", valueGroups));
        return sql.toString();
    }
    
    // 统一的批量更新方法入口
    public String buildBatchUpdateSql(int batchSize) {
        return buildBatchUpdateSql(batchSize, "mysql"); // 默认使用MySQL
    }
    
    public String buildBatchUpdateSql(int batchSize, String databaseType) {
        return buildOptimalBatchUpdateSql(batchSize, databaseType);
    }
    
    // VALUES子句批量更新（MySQL 8.0+/PostgreSQL）
    public String buildBatchUpdateWithValues(int batchSize) {
        validateTableName();
        validateFieldsAndValues();
        validatePrimaryKeys();
        
        List<String> updateFields = fields.stream()
            .filter(field -> !primaryKeys.contains(field))
            .collect(Collectors.toList());
            
        if (updateFields.isEmpty()) {
            throw new IllegalArgumentException("No fields to update (all fields are primary keys)");
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        
        List<String> setClauses = new ArrayList<>();
        for (String field : updateFields) {
            setClauses.add(field + " = temp_values." + field);
        }
        sql.append(String.join(", ", setClauses));
        
        sql.append(" FROM (VALUES ");
        List<String> valueRows = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            List<String> rowValues = new ArrayList<>();
            for (String field : fields) {
                rowValues.add("#{paramsList[" + i + "]." + field + "}");
            }
            valueRows.add("(" + String.join(", ", rowValues) + ")");
        }
        sql.append(String.join(", ", valueRows));
        sql.append(") AS temp_values(").append(joinFields(fields)).append(")");
        
        sql.append(" WHERE ");
        List<String> joinConditions = new ArrayList<>();
        for (String pk : primaryKeys) {
            joinConditions.add(tableName + "." + pk + " = temp_values." + pk);
        }
        sql.append(String.join(" AND ", joinConditions));
        
        return sql.toString();
    }
    
    // 多条UPDATE语句（兼容性好）
    public String buildBatchUpdateSimple(int batchSize) {
        validateTableName();
        validateFieldsAndValues();
        validatePrimaryKeys();
        
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < batchSize; i++) {
            if (i > 0) sql.append("; ");
            sql.append("UPDATE ").append(tableName).append(" SET ");
            
            List<String> setClauses = new ArrayList<>();
            for (String field : fields) {
                if (!primaryKeys.contains(field)) {
                    setClauses.add(field + " = #{paramsList[" + i + "]." + field + "}");
                }
            }
            sql.append(String.join(", ", setClauses));
            
            sql.append(" WHERE ");
            List<String> whereConds = new ArrayList<>();
            for (String pk : primaryKeys) {
                whereConds.add(pk + " = #{paramsList[" + i + "]." + pk + "}");
            }
            sql.append(String.join(" AND ", whereConds));
        }
        return sql.toString();
    }
    
    // MERGE语句（SQL Server/Oracle）
    public String buildBatchMergeSQL(int batchSize) {
        validateTableName();
        validateFieldsAndValues();
        validatePrimaryKeys();
        
        StringBuilder sql = new StringBuilder();
        sql.append("MERGE ").append(tableName).append(" AS target USING (");
        
        List<String> valueRows = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            List<String> rowValues = new ArrayList<>();
            for (String field : fields) {
                rowValues.add("#{paramsList[" + i + "]." + field + "}");
            }
            valueRows.add("(" + String.join(", ", rowValues) + ")");
        }
        sql.append("VALUES ").append(String.join(", ", valueRows));
        sql.append(") AS source(").append(joinFields(fields)).append(") ON ");
        
        List<String> matchConditions = new ArrayList<>();
        for (String pk : primaryKeys) {
            matchConditions.add("target." + pk + " = source." + pk);
        }
        sql.append(String.join(" AND ", matchConditions));
        
        sql.append(" WHEN MATCHED THEN UPDATE SET ");
        List<String> updateClauses = new ArrayList<>();
        for (String field : fields) {
            if (!primaryKeys.contains(field)) {
                updateClauses.add("target." + field + " = source." + field);
            }
        }
        sql.append(String.join(", ", updateClauses));
        
        return sql.toString();
    }
    
    // 智能选择批量更新策略
    public String buildOptimalBatchUpdateSql(int batchSize, String databaseType) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        
        if (batchSize == 1) {
            return buildUpdateSql();
        } else if (batchSize <= 10) {
            return buildBatchUpdateSimple(batchSize);
        } else {
            switch (databaseType.toLowerCase()) {
                case "mysql":
                    if (batchSize <= 100) {
                        return buildBatchUpdateWithValues(batchSize);
                    } else {
                        throw new IllegalArgumentException("Batch size too large, consider splitting into smaller batches");
                    }
                case "postgresql":
                    return buildBatchUpdateWithValues(batchSize);
                case "sqlserver":
                case "oracle":
                    return buildBatchMergeSQL(batchSize);
                default:
                    return buildBatchUpdateSimple(batchSize);
            }
        }
    }
}
