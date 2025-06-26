@Service
public class LoadTaskService {
    @Autowired
    private LoadTaskMapper loadTaskMapper;
    
    /**
     * 执行加载任务
     * @param taskCode 任务编码
     * @param dataList 数据列表
     * @return 影响的行数
     */
    public int executeLoadTask(String taskCode, List<Map<String, Object>> dataList) {
        // 1. 获取任务配置
        LoadTask task = loadTaskMapper.getTaskByCode(taskCode);
        if (task == null) {
            throw new RuntimeException("任务不存在: " + taskCode);
        }
        
        // 2. 获取字段映射
        List<FieldMapping> fieldMappings = loadTaskMapper.getFieldMappingsByTaskId(task.getId());
        if (fieldMappings.isEmpty()) {
            throw new RuntimeException("任务未配置字段映射: " + taskCode);
        }
        
        // 3. 获取条件配置
        List<ConditionConfig> conditions = loadTaskMapper.getConditionsByTaskId(task.getId());
        
        // 4. 构建SQL
        int affectedRows = 0;
        for (Map<String, Object> data : dataList) {
            SqlBuilder sqlBuilder = buildSqlFromConfig(task, fieldMappings, conditions, data);
            
            // 5. 执行SQL
            if ("UPSERT".equals(task.getOperationType())) {
                String sql = sqlBuilder.buildUpsertSql();
                affectedRows += loadTaskMapper.executeInsertSql(sql, data);
            } else if ("DELETE".equals(task.getOperationType())) {
                String sql = sqlBuilder.buildDeleteSql();
                affectedRows += loadTaskMapper.executeDeleteSql(sql, data);
            }
        }
        
        return affectedRows;
    }
    
    /**
     * 根据配置构建SQL构建器
     */
    private SqlBuilder buildSqlFromConfig(LoadTask task, List<FieldMapping> fieldMappings, 
                                         List<ConditionConfig> conditions, Map<String, Object> data) {
        SqlBuilder sqlBuilder = new SqlBuilder();
        sqlBuilder.setTableName(task.getTargetTable());
        sqlBuilder.setOperationType(task.getOperationType());
        
        List<String> fields = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        List<String> whereConditions = new ArrayList<>();
        
        // 处理字段映射
        for (FieldMapping mapping : fieldMappings) {
            String sourceField = mapping.getSourceField();
            String targetField = mapping.getTargetField();
            
            // 检查数据中是否包含该字段
            if (data.containsKey(sourceField) || mapping.getDefaultValue() != null) {
                fields.add(targetField);
                
                Object value = data.getOrDefault(sourceField, mapping.getDefaultValue());
                // 根据字段类型处理值
                String formattedValue = formatValueByType(value, mapping.getFieldType());
                values.add(formattedValue);
                
                // 记录主键字段
                if (mapping.getIsPrimary() == 1) {
                    primaryKeys.add(targetField);
                }
            } else if (mapping.getIsRequired() == 1) {
                throw new RuntimeException("必填字段缺失: " + sourceField);
            }
        }
        
        // 处理条件配置
        for (ConditionConfig condition : conditions) {
            if ("WHERE".equals(condition.getConditionType())) {
                String conditionStr = buildConditionString(condition, data);
                if (conditionStr != null) {
                    whereConditions.add(conditionStr);
                }
            }
        }
        
        sqlBuilder.setFields(fields);
        sqlBuilder.setValues(values);
        sqlBuilder.setPrimaryKeys(primaryKeys);
        sqlBuilder.setWhereConditions(whereConditions);
        
        return sqlBuilder;
    }
    
    /**
     * 根据字段类型格式化值
     */
    private String formatValueByType(Object value, String fieldType) {
        if (value == null) {
            return "NULL";
        }
        
        switch (fieldType.toUpperCase()) {
            case "STRING":
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return "'" + value.toString().replace("'", "''") + "'";
            case "DATE":
                // 处理日期格式
                if (value instanceof Date) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    return "'" + sdf.format((Date) value) + "'";
                }
                return "'" + value.toString() + "'";
            case "DATETIME":
            case "TIMESTAMP":
                // 处理日期时间格式
                if (value instanceof Date) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    return "'" + sdf.format((Date) value) + "'";
                }
                return "'" + value.toString() + "'";
            case "INTEGER":
            case "INT":
            case "BIGINT":
            case "LONG":
            case "DOUBLE":
            case "FLOAT":
            case "DECIMAL":
                return value.toString();
            case "BOOLEAN":
                return Boolean.parseBoolean(value.toString()) ? "1" : "0";
            default:
                return "'" + value.toString() + "'";
        }
    }
    
    /**
     * 构建条件字符串
     */
    private String buildConditionString(ConditionConfig condition, Map<String, Object> data) {
        String fieldName = condition.getFieldName();
        String operator = condition.getOperator();
        String conditionValue = condition.getConditionValue();
        
        // 如果条件值是动态的（从数据中获取）
        if (conditionValue != null && conditionValue.startsWith("${") && conditionValue.endsWith("}")) {
            String dataField = conditionValue.substring(2, conditionValue.length() - 1);
            if (data.containsKey(dataField)) {
                conditionValue = data.get(dataField).toString();
            } else {
                // 如果数据中不包含该字段，则跳过该条件
                return null;
            }
        }
        
        // 构建条件
        StringBuilder conditionStr = new StringBuilder();
        conditionStr.append(fieldName).append(" ").append(operator).append(" ");
        
        // 根据操作符处理条件值
        if ("IN".equalsIgnoreCase(operator)) {
            conditionStr.append("(").append(conditionValue).append(")");
        } else if ("LIKE".equalsIgnoreCase(operator)) {
            conditionStr.append("'%").append(conditionValue).append("%'");
        } else if ("IS NULL".equalsIgnoreCase(operator) || "IS NOT NULL".equalsIgnoreCase(operator)) {
            // 对于IS NULL和IS NOT NULL，不需要条件值
            conditionStr = new StringBuilder(fieldName + " " + operator);
        } else {
            // 其他操作符
            if (conditionValue.matches("^\d+(\.\d+)?$")) {
                // 数字不需要引号
                conditionStr.append(conditionValue);
            } else {
                // 字符串需要引号
                conditionStr.append("'").append(conditionValue).append("'");
            }
        }
        
        return conditionStr.toString();
    }
}
