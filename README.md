我来帮你设计一个完整的ETL聚合组件。这个设计包括配置表结构、配置实体、聚合引擎和使用示例。

## 1. 数据库配置表设计

```sql
-- 聚合配置主表
CREATE TABLE etl_aggregate_config (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_name VARCHAR(100) NOT NULL COMMENT '配置名称',
    description VARCHAR(500) COMMENT '描述',
    filter_expression VARCHAR(2000) COMMENT '过滤表达式，如: amount>100 AND status=active',
    enabled TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 分组字段配置表
CREATE TABLE etl_aggregate_group_field (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_id BIGINT NOT NULL COMMENT '配置ID',
    field_name VARCHAR(100) NOT NULL COMMENT '分组字段名',
    sort_order INT DEFAULT 0 COMMENT '排序',
    FOREIGN KEY (config_id) REFERENCES etl_aggregate_config(id)
);

-- 聚合字段配置表
CREATE TABLE etl_aggregate_field (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_id BIGINT NOT NULL COMMENT '配置ID',
    field_name VARCHAR(100) NOT NULL COMMENT '聚合字段名',
    aggregate_type VARCHAR(20) NOT NULL COMMENT '聚合类型: SUM/AVG/COUNT/MAX/MIN',
    output_field_name VARCHAR(100) COMMENT '输出字段名，为空则使用原字段名',
    sort_order INT DEFAULT 0 COMMENT '排序',
    FOREIGN KEY (config_id) REFERENCES etl_aggregate_config(id)
);
```

## 2. 配置实体类

```java
import lombok.Data;
import java.util.List;

/**
 * 聚合配置
 */
@Data
public class AggregateConfig {
    private Long id;
    private String configName;
    private String description;
    private String filterExpression;
    private Boolean enabled;
    private List<String> groupByFields;
    private List<AggregateField> aggregateFields;
}

/**
 * 聚合字段配置
 */
@Data
public class AggregateField {
    private String fieldName;
    private AggregateType aggregateType;
    private String outputFieldName;
    
    public String getOutputFieldName() {
        return outputFieldName != null ? outputFieldName : fieldName;
    }
}

/**
 * 聚合类型枚举
 */
public enum AggregateType {
    SUM,    // 求和
    AVG,    // 平均值
    COUNT,  // 计数
    MAX,    // 最大值
    MIN     // 最小值
}
```

## 3. 核心聚合引擎

```java
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ETL聚合组件
 */
public class AggregateEngine {
    
    private final AggregateConfig config;
    
    public AggregateEngine(AggregateConfig config) {
        this.config = config;
    }
    
    /**
     * 执行聚合
     * @param inputData 输入数据
     * @return 聚合后的数据
     */
    public List<Map<String, Object>> aggregate(List<Map<String, Object>> inputData) {
        // 1. 过滤数据
        List<Map<String, Object>> filteredData = filterData(inputData);
        
        // 2. 按分组字段分组
        Map<String, List<Map<String, Object>>> groupedData = groupData(filteredData);
        
        // 3. 对每个分组执行聚合
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : groupedData.entrySet()) {
            Map<String, Object> aggregatedRow = aggregateGroup(entry.getValue());
            result.add(aggregatedRow);
        }
        
        return result;
    }
    
    /**
     * 过滤数据
     */
    private List<Map<String, Object>> filterData(List<Map<String, Object>> inputData) {
        if (config.getFilterExpression() == null || config.getFilterExpression().trim().isEmpty()) {
            return inputData;
        }
        
        return inputData.stream()
                .filter(row -> evaluateFilter(row, config.getFilterExpression()))
                .collect(Collectors.toList());
    }
    
    /**
     * 评估过滤表达式
     */
    private boolean evaluateFilter(Map<String, Object> row, String filterExpression) {
        try {
            // 使用简单的表达式解析器
            FilterEvaluator evaluator = new FilterEvaluator(row);
            return evaluator.evaluate(filterExpression);
        } catch (Exception e) {
            // 过滤表达式错误时，默认不过滤
            System.err.println("过滤表达式解析错误: " + e.getMessage());
            return true;
        }
    }
    
    /**
     * 分组数据
     */
    private Map<String, List<Map<String, Object>>> groupData(List<Map<String, Object>> data) {
        return data.stream()
                .collect(Collectors.groupingBy(this::buildGroupKey));
    }
    
    /**
     * 构建分组键
     */
    private String buildGroupKey(Map<String, Object> row) {
        List<String> groupByFields = config.getGroupByFields();
        if (groupByFields == null || groupByFields.isEmpty()) {
            return "DEFAULT_GROUP";
        }
        
        StringBuilder keyBuilder = new StringBuilder();
        for (String field : groupByFields) {
            Object value = row.get(field);
            keyBuilder.append(field).append("=")
                     .append(value == null ? "null" : value.toString())
                     .append("|");
        }
        return keyBuilder.toString();
    }
    
    /**
     * 对单个分组执行聚合
     */
    private Map<String, Object> aggregateGroup(List<Map<String, Object>> groupData) {
        Map<String, Object> result = new LinkedHashMap<>();
        
        if (groupData.isEmpty()) {
            return result;
        }
        
        // 1. 添加分组字段
        Map<String, Object> firstRow = groupData.get(0);
        for (String groupField : config.getGroupByFields()) {
            result.put(groupField, firstRow.get(groupField));
        }
        
        // 2. 执行聚合计算
        for (AggregateField aggField : config.getAggregateFields()) {
            Object aggregatedValue = calculateAggregate(groupData, aggField);
            result.put(aggField.getOutputFieldName(), aggregatedValue);
        }
        
        return result;
    }
    
    /**
     * 计算聚合值
     */
    private Object calculateAggregate(List<Map<String, Object>> groupData, AggregateField aggField) {
        String fieldName = aggField.getFieldName();
        AggregateType type = aggField.getAggregateType();
        
        switch (type) {
            case SUM:
                return calculateSum(groupData, fieldName);
            case AVG:
                return calculateAvg(groupData, fieldName);
            case COUNT:
                return groupData.size();
            case MAX:
                return calculateMax(groupData, fieldName);
            case MIN:
                return calculateMin(groupData, fieldName);
            default:
                throw new IllegalArgumentException("不支持的聚合类型: " + type);
        }
    }
    
    private BigDecimal calculateSum(List<Map<String, Object>> data, String fieldName) {
        return data.stream()
                .map(row -> row.get(fieldName))
                .filter(Objects::nonNull)
                .map(this::toBigDecimal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
    
    private BigDecimal calculateAvg(List<Map<String, Object>> data, String fieldName) {
        List<BigDecimal> values = data.stream()
                .map(row -> row.get(fieldName))
                .filter(Objects::nonNull)
                .map(this::toBigDecimal)
                .collect(Collectors.toList());
        
        if (values.isEmpty()) {
            return BigDecimal.ZERO;
        }
        
        BigDecimal sum = values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return sum.divide(BigDecimal.valueOf(values.size()), 2, RoundingMode.HALF_UP);
    }
    
    private Comparable calculateMax(List<Map<String, Object>> data, String fieldName) {
        return data.stream()
                .map(row -> row.get(fieldName))
                .filter(Objects::nonNull)
                .map(obj -> (Comparable) obj)
                .max(Comparator.naturalOrder())
                .orElse(null);
    }
    
    private Comparable calculateMin(List<Map<String, Object>> data, String fieldName) {
        return data.stream()
                .map(row -> row.get(fieldName))
                .filter(Objects::nonNull)
                .map(obj -> (Comparable) obj)
                .min(Comparator.naturalOrder())
                .orElse(null);
    }
    
    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            return new BigDecimal(value.toString());
        }
    }
}
```

## 4. 过滤表达式解析器

```java
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 简单的过滤表达式解析器
 * 支持：=, !=, >, <, >=, <=, AND, OR
 */
public class FilterEvaluator {
    
    private final Map<String, Object> row;
    
    public FilterEvaluator(Map<String, Object> row) {
        this.row = row;
    }
    
    public boolean evaluate(String expression) {
        // 处理 AND 和 OR
        if (expression.contains(" OR ")) {
            String[] parts = expression.split(" OR ");
            for (String part : parts) {
                if (evaluate(part.trim())) {
                    return true;
                }
            }
            return false;
        }
        
        if (expression.contains(" AND ")) {
            String[] parts = expression.split(" AND ");
            for (String part : parts) {
                if (!evaluate(part.trim())) {
                    return false;
                }
            }
            return true;
        }
        
        // 处理单个条件
        return evaluateSingleCondition(expression.trim());
    }
    
    private boolean evaluateSingleCondition(String condition) {
        // 支持的操作符：>=, <=, !=, =, >, <
        Pattern pattern = Pattern.compile("([\\w.]+)\\s*(>=|<=|!=|=|>|<)\\s*(.+)");
        Matcher matcher = pattern.matcher(condition);
        
        if (!matcher.matches()) {
            return true; // 无法解析的条件默认为真
        }
        
        String fieldName = matcher.group(1).trim();
        String operator = matcher.group(2).trim();
        String valueStr = matcher.group(3).trim();
        
        Object fieldValue = row.get(fieldName);
        if (fieldValue == null) {
            return false;
        }
        
        return compareValues(fieldValue, operator, valueStr);
    }
    
    private boolean compareValues(Object fieldValue, String operator, String valueStr) {
        // 去除引号
        valueStr = valueStr.replaceAll("^['\"]|['\"]$", "");
        
        switch (operator) {
            case "=":
                return fieldValue.toString().equals(valueStr);
            case "!=":
                return !fieldValue.toString().equals(valueStr);
            case ">":
                return compareNumeric(fieldValue, valueStr) > 0;
            case "<":
                return compareNumeric(fieldValue, valueStr) < 0;
            case ">=":
                return compareNumeric(fieldValue, valueStr) >= 0;
            case "<=":
                return compareNumeric(fieldValue, valueStr) <= 0;
            default:
                return false;
        }
    }
    
    private int compareNumeric(Object fieldValue, String valueStr) {
        try {
            double fieldNum = Double.parseDouble(fieldValue.toString());
            double compareNum = Double.parseDouble(valueStr);
            return Double.compare(fieldNum, compareNum);
        } catch (NumberFormatException e) {
            return fieldValue.toString().compareTo(valueStr);
        }
    }
}
```

## 5. 使用示例

```java
import java.util.*;

public class AggregateEngineDemo {
    
    public static void main(String[] args) {
        // 1. 准备测试数据
        List<Map<String, Object>> inputData = prepareTestData();
        
        // 2. 配置聚合规则
        AggregateConfig config = new AggregateConfig();
        config.setGroupByFields(Arrays.asList("category", "region"));
        config.setFilterExpression("amount > 100");
        
        // 配置聚合字段
        List<AggregateField> aggFields = new ArrayList<>();
        
        AggregateField sumField = new AggregateField();
        sumField.setFieldName("amount");
        sumField.setAggregateType(AggregateType.SUM);
        sumField.setOutputFieldName("total_amount");
        aggFields.add(sumField);
        
        AggregateField avgField = new AggregateField();
        avgField.setFieldName("amount");
        avgField.setAggregateType(AggregateType.AVG);
        avgField.setOutputFieldName("avg_amount");
        aggFields.add(avgField);
        
        AggregateField countField = new AggregateField();
        countField.setFieldName("amount");
        countField.setAggregateType(AggregateType.COUNT);
        countField.setOutputFieldName("count");
        aggFields.add(countField);
        
        config.setAggregateFields(aggFields);
        
        // 3. 执行聚合
        AggregateEngine engine = new AggregateEngine(config);
        List<Map<String, Object>> result = engine.aggregate(inputData);
        
        // 4. 输出结果
        System.out.println("聚合结果：");
        for (Map<String, Object> row : result) {
            System.out.println(row);
        }
    }
    
    private static List<Map<String, Object>> prepareTestData() {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new HashMap<>();
        row1.put("category", "A");
        row1.put("region", "North");
        row1.put("amount", 150);
        data.add(row1);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put("category", "A");
        row2.put("region", "North");
        row2.put("amount", 200);
        data.add(row2);
        
        Map<String, Object> row3 = new HashMap<>();
        row3.put("category", "A");
        row3.put("region", "South");
        row3.put("amount", 120);
        data.add(row3);
        
        Map<String, Object> row4 = new HashMap<>();
        row4.put("category", "B");
        row4.put("region", "North");
        row4.put("amount", 80); // 会被过滤掉
        data.add(row4);
        
        Map<String, Object> row5 = new HashMap<>();
        row5.put("category", "B");
        row5.put("region", "North");
        row5.put("amount", 300);
        data.add(row5);
        
        return data;
    }
}
```

## 6. 配置管理服务

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

/**
 * 聚合配置服务
 */
@Service
public class AggregateConfigService {
    
    @Autowired
    private AggregateConfigMapper configMapper;
    
    @Autowired
    private AggregateGroupFieldMapper groupFieldMapper;
    
    @Autowired
    private AggregateFieldMapper aggregateFieldMapper;
    
    /**
     * 根据配置ID加载完整配置
     */
    public AggregateConfig loadConfig(Long configId) {
        AggregateConfig config = configMapper.selectById(configId);
        if (config == null) {
            throw new RuntimeException("配置不存在: " + configId);
        }
        
        // 加载分组字段
        List<String> groupFields = groupFieldMapper.selectByConfigId(configId);
        config.setGroupByFields(groupFields);
        
        // 加载聚合字段
        List<AggregateField> aggFields = aggregateFieldMapper.selectByConfigId(configId);
        config.setAggregateFields(aggFields);
        
        return config;
    }
    
    /**
     * 执行聚合
     */
    public List<Map<String, Object>> executeAggregate(Long configId, List<Map<String, Object>> inputData) {
        AggregateConfig config = loadConfig(configId);
        
        if (!config.getEnabled()) {
            throw new RuntimeException("配置未启用: " + configId);
        }
        
        AggregateEngine engine = new AggregateEngine(config);
        return engine.aggregate(inputData);
    }
}
```

## 主要特性

✅ **灵活的分组**：支持多字段分组  
✅ **多种聚合函数**：SUM、AVG、COUNT、MAX、MIN  
✅ **数据过滤**：支持复杂的过滤表达式（AND、OR、比较操作符）  
✅ **可配置化**：所有配置存储在数据库中  
✅ **自定义输出字段名**：聚合结果可以指定输出字段名  
✅ **类型安全**：使用BigDecimal进行数值计算，避免精度丢失  

这个设计可以根据你的实际需求进行扩展，比如：
- 添加更多聚合函数（DISTINCT_COUNT、MEDIAN等）
- 支持更复杂的过滤表达式（使用MVEL或SpEL）
- 支持嵌套聚合
- 添加性能优化（并行处理）

需要我进一步完善某个部分吗？
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 增强的过滤表达式解析器
 * 支持：=, !=, >, <, >=, <=, IN, NOT IN, AND, OR, IS NULL, IS NOT NULL
 */
public class FilterEvaluator {
    
    private final Map<String, Object> row;
    
    public FilterEvaluator(Map<String, Object> row) {
        this.row = row;
    }
    
    public boolean evaluate(String expression) {
        if (expression == null || expression.trim().isEmpty()) {
            return true;
        }
        
        // 处理 OR（优先级最低）
        if (expression.contains(" OR ")) {
            String[] parts = splitByOperator(expression, " OR ");
            for (String part : parts) {
                if (evaluate(part.trim())) {
                    return true;
                }
            }
            return false;
        }
        
        // 处理 AND
        if (expression.contains(" AND ")) {
            String[] parts = splitByOperator(expression, " AND ");
            for (String part : parts) {
                if (!evaluate(part.trim())) {
                    return false;
                }
            }
            return true;
        }
        
        // 处理括号
        if (expression.startsWith("(") && expression.endsWith(")")) {
            return evaluate(expression.substring(1, expression.length() - 1));
        }
        
        // 处理单个条件
        return evaluateSingleCondition(expression.trim());
    }
    
    /**
     * 分割表达式，考虑括号内的内容
     */
    private String[] splitByOperator(String expression, String operator) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        int parenthesesLevel = 0;
        
        for (int i = 0; i <= expression.length() - operator.length(); i++) {
            char c = expression.charAt(i);
            
            if (c == '(') {
                parenthesesLevel++;
            } else if (c == ')') {
                parenthesesLevel--;
            }
            
            if (parenthesesLevel == 0 && expression.substring(i).startsWith(operator)) {
                parts.add(expression.substring(start, i));
                start = i + operator.length();
                i += operator.length() - 1;
            }
        }
        parts.add(expression.substring(start));
        
        return parts.toArray(new String[0]);
    }
    
    private boolean evaluateSingleCondition(String condition) {
        condition = condition.trim();
        
        // 处理 IS NULL
        if (condition.toUpperCase().matches(".*\\s+IS\\s+NULL$")) {
            Pattern pattern = Pattern.compile("([\\w.]+)\\s+IS\\s+NULL", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(condition);
            if (matcher.matches()) {
                String fieldName = matcher.group(1).trim();
                return row.get(fieldName) == null;
            }
        }
        
        // 处理 IS NOT NULL
        if (condition.toUpperCase().matches(".*\\s+IS\\s+NOT\\s+NULL$")) {
            Pattern pattern = Pattern.compile("([\\w.]+)\\s+IS\\s+NOT\\s+NULL", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(condition);
            if (matcher.matches()) {
                String fieldName = matcher.group(1).trim();
                return row.get(fieldName) != null;
            }
        }
        
        // 处理 NOT IN
        if (condition.toUpperCase().contains(" NOT IN ")) {
            return evaluateNotInCondition(condition);
        }
        
        // 处理 IN
        if (condition.toUpperCase().contains(" IN ")) {
            return evaluateInCondition(condition);
        }
        
        // 处理其他比较操作符
        return evaluateComparisonCondition(condition);
    }
    
    /**
     * 处理 IN 条件
     * 格式：fieldName IN (value1, value2, value3)
     */
    private boolean evaluateInCondition(String condition) {
        Pattern pattern = Pattern.compile("([\\w.]+)\\s+IN\\s+\\((.+?)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(condition);
        
        if (!matcher.matches()) {
            System.err.println("IN 条件格式错误: " + condition);
            return false;
        }
        
        String fieldName = matcher.group(1).trim();
        String valuesStr = matcher.group(2).trim();
        
        Object fieldValue = row.get(fieldName);
        if (fieldValue == null) {
            return false;
        }
        
        // 解析 IN 列表中的值
        List<String> inValues = parseInValues(valuesStr);
        
        // 检查字段值是否在列表中
        String fieldValueStr = fieldValue.toString();
        for (String inValue : inValues) {
            if (fieldValueStr.equals(inValue)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 处理 NOT IN 条件
     */
    private boolean evaluateNotInCondition(String condition) {
        Pattern pattern = Pattern.compile("([\\w.]+)\\s+NOT\\s+IN\\s+\\((.+?)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(condition);
        
        if (!matcher.matches()) {
            System.err.println("NOT IN 条件格式错误: " + condition);
            return false;
        }
        
        String fieldName = matcher.group(1).trim();
        String valuesStr = matcher.group(2).trim();
        
        Object fieldValue = row.get(fieldName);
        if (fieldValue == null) {
            return true; // NULL 不在任何列表中
        }
        
        // 解析 NOT IN 列表中的值
        List<String> inValues = parseInValues(valuesStr);
        
        // 检查字段值是否不在列表中
        String fieldValueStr = fieldValue.toString();
        for (String inValue : inValues) {
            if (fieldValueStr.equals(inValue)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 解析 IN 子句中的值列表
     * 支持：'value1', 'value2', 123, 456
     */
    private List<String> parseInValues(String valuesStr) {
        List<String> values = new ArrayList<>();
        
        // 分割逗号，但要考虑引号内的逗号
        StringBuilder currentValue = new StringBuilder();
        boolean inQuote = false;
        char quoteChar = 0;
        
        for (int i = 0; i < valuesStr.length(); i++) {
            char c = valuesStr.charAt(i);
            
            if ((c == '\'' || c == '"') && !inQuote) {
                inQuote = true;
                quoteChar = c;
            } else if (c == quoteChar && inQuote) {
                inQuote = false;
                quoteChar = 0;
            } else if (c == ',' && !inQuote) {
                String value = currentValue.toString().trim();
                if (!value.isEmpty()) {
                    values.add(cleanValue(value));
                }
                currentValue = new StringBuilder();
            } else {
                currentValue.append(c);
            }
        }
        
        // 添加最后一个值
        String lastValue = currentValue.toString().trim();
        if (!lastValue.isEmpty()) {
            values.add(cleanValue(lastValue));
        }
        
        return values;
    }
    
    /**
     * 清理值（去除引号和空格）
     */
    private String cleanValue(String value) {
        value = value.trim();
        // 去除首尾的单引号或双引号
        if ((value.startsWith("'") && value.endsWith("'")) ||
            (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
    
    /**
     * 处理比较条件：>=, <=, !=, =, >, <
     */
    private boolean evaluateComparisonCondition(String condition) {
        Pattern pattern = Pattern.compile("([\\w.]+)\\s*(>=|<=|!=|=|>|<)\\s*(.+)");
        Matcher matcher = pattern.matcher(condition);
        
        if (!matcher.matches()) {
            System.err.println("条件格式错误: " + condition);
            return true;
        }
        
        String fieldName = matcher.group(1).trim();
        String operator = matcher.group(2).trim();
        String valueStr = matcher.group(3).trim();
        
        Object fieldValue = row.get(fieldName);
        if (fieldValue == null) {
            return false;
        }
        
        return compareValues(fieldValue, operator, valueStr);
    }
    
    private boolean compareValues(Object fieldValue, String operator, String valueStr) {
        valueStr = cleanValue(valueStr);
        
        switch (operator) {
            case "=":
                return fieldValue.toString().equals(valueStr);
            case "!=":
                return !fieldValue.toString().equals(valueStr);
            case ">":
                return compareNumeric(fieldValue, valueStr) > 0;
            case "<":
                return compareNumeric(fieldValue, valueStr) < 0;
            case ">=":
                return compareNumeric(fieldValue, valueStr) >= 0;
            case "<=":
                return compareNumeric(fieldValue, valueStr) <= 0;
            default:
                return false;
        }
    }
    
    private int compareNumeric(Object fieldValue, String valueStr) {
        try {
            double fieldNum = Double.parseDouble(fieldValue.toString());
            double compareNum = Double.parseDouble(valueStr);
            return Double.compare(fieldNum, compareNum);
        } catch (NumberFormatException e) {
            return fieldValue.toString().compareTo(valueStr);
        }
    }
}
