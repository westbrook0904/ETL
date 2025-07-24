package com.dataloader.service;

import com.dataloader.config.LoaderConfig;
import com.dataloader.config.FieldMapping;
import com.dataloader.mapper.DataLoaderMapper;
import com.dataloader.sql.SqlBuilder;
import com.dataloader.exception.DataLoaderException;
import com.dataloader.validator.DataValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * 数据加载服务 - 支持单条和批量数据处理
 */
@Service
public class DataLoaderService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLoaderService.class);
    
    @Autowired
    private DataLoaderMapper dataLoaderMapper;
    
    @Autowired
    private ConfigService configService;
    
    // ==================== 主要业务方法 ====================
    
    /**
     * 根据配置ID执行数据加载
     */
    @Transactional(rollbackFor = Exception.class)
    public LoadResult loadDataByConfigId(String configId, List<Map<String, Object>> dataList) {
        logger.info("开始执行数据加载，配置ID: {}, 数据量: {}", configId, dataList != null ? dataList.size() : 0);
        
        try {
            LoaderConfig config = configService.getLoaderConfig(configId);
            if (config == null) {
                throw new DataLoaderException("CONFIG_NOT_FOUND", "找不到配置: " + configId, configId);
            }
            
            return loadData(config, dataList);
        } catch (DataLoaderException e) {
            logger.error("数据加载失败，配置ID: {}, 错误: {}", configId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("数据加载出现未知错误，配置ID: {}", configId, e);
            throw new DataLoaderException("UNKNOWN_ERROR", "数据加载失败: " + e.getMessage(), configId);
        }
    }
    
    /**
     * 执行数据加载
     */
    @Transactional(rollbackFor = Exception.class)
    public LoadResult loadData(LoaderConfig config, List<Map<String, Object>> dataList) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 数据校验
            DataValidator.validateConfigAndData(config, dataList);
            logger.info("数据校验通过，配置: {}, 数据量: {}", config.getConfigId(), dataList.size());
            
            LoadResult result = new LoadResult();
            result.setConfigId(config.getConfigId());
            result.setTotalRecords(dataList.size());
            result.setStartTime(startTime);
            
            if (dataList.isEmpty()) {
                result.setSuccessRecords(0);
                result.setFailedRecords(0);
                result.setEndTime(System.currentTimeMillis());
                logger.info("数据加载完成（空数据），配置: {}", config.getConfigId());
                return result;
            }
            
            // 分批处理数据
            int batchSize = config.getBatchSize();
            int totalProcessed = 0;
            int totalFailed = 0;
            
            logger.info("开始分批处理数据，批次大小: {}, 总批次数: {}", batchSize, 
                       (dataList.size() + batchSize - 1) / batchSize);
            
            for (int i = 0; i < dataList.size(); i += batchSize) {
                int batchNumber = i / batchSize + 1;
                int endIndex = Math.min(i + batchSize, dataList.size());
                List<Map<String, Object>> batch = dataList.subList(i, endIndex);
                
                try {
                    logger.debug("处理第{}批数据，范围: {}-{}", batchNumber, i + 1, endIndex);
                    int processed = processBatch(config, batch);
                    totalProcessed += processed;
                    logger.debug("第{}批数据处理完成，影响行数: {}", batchNumber, processed);
                } catch (Exception e) {
                    totalFailed += batch.size();
                    String errorMsg = String.format("第%d批数据处理失败: %s", batchNumber, e.getMessage());
                    result.addError(errorMsg);
                    logger.error(errorMsg, e);
                    
                    if (!config.isEnableTransaction()) {
                        continue;
                    } else {
                        throw new DataLoaderException("BATCH_PROCESSING_FAILED", errorMsg, config.getConfigId());
                    }
                }
            }
            
            result.setSuccessRecords(totalProcessed);
            result.setFailedRecords(totalFailed);
            result.setEndTime(System.currentTimeMillis());
            
            logger.info("数据加载完成，配置: {}, 成功: {}, 失败: {}, 耗时: {}ms", 
                       config.getConfigId(), totalProcessed, totalFailed, 
                       result.getEndTime() - result.getStartTime());
            
            return result;
            
        } catch (DataLoaderException e) {
            logger.error("数据加载失败，配置: {}, 错误: {}", config.getConfigId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("数据加载出现未知错误，配置: {}", config.getConfigId(), e);
            throw new DataLoaderException("UNKNOWN_ERROR", "数据加载失败: " + e.getMessage(), config.getConfigId());
        }
    }
    
    // ==================== 批量插入相关方法 ====================
    
    /**
     * 批量插入并返回生成的主键
     */
    public BatchInsertResult batchInsertWithKeys(String configId, List<Map<String, Object>> dataList) {
        try {
            LoaderConfig config = configService.getLoaderConfig(configId);
            validateConfig(config);
            validateData(dataList);
            
            if (!"INSERT".equalsIgnoreCase(config.getLoadType().getValue())) {
                throw new IllegalArgumentException("Config must be INSERT type for batch insert");
            }
            
            // 准备批量插入数据
            List<Map<String, Object>> processedDataList = new ArrayList<>();
            List<String> fields = extractFields(config);
            
            // 处理每行数据
            for (Map<String, Object> row : dataList) {
                Map<String, Object> processedRow = buildParameters(config, row);
                processedDataList.add(processedRow);
            }
            
            // 执行批量插入
            int affectedRows = dataLoaderMapper.batchInsertWithKeys(
                config.getTableName(), 
                fields, 
                processedDataList
            );
            
            // 提取生成的主键
            List<Long> generatedKeys = extractGeneratedKeys(processedDataList);
            
            return new BatchInsertResult(affectedRows, generatedKeys);
            
        } catch (Exception e) {
            return BatchInsertResult.failure(e.getMessage());
        }
    }
    
    /**
     * 优化的批量插入（支持大数据量分批处理）
     */
    public BatchInsertResult batchInsertOptimized(String configId, List<Map<String, Object>> dataList, int batchSize) {
        if (batchSize <= 0) {
            batchSize = 1000; // 默认批次大小
        }
        
        List<Long> allGeneratedKeys = new ArrayList<>();
        int totalAffectedRows = 0;
        
        try {
            // 分批处理
            for (int i = 0; i < dataList.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, dataList.size());
                List<Map<String, Object>> batch = dataList.subList(i, endIndex);
                
                BatchInsertResult batchResult = batchInsertWithKeys(configId, batch);
                if (!batchResult.isSuccess()) {
                    throw new RuntimeException("Batch insert failed: " + batchResult.getErrorMessage());
                }
                
                totalAffectedRows += batchResult.getAffectedRows();
                if (batchResult.getGeneratedKeys() != null) {
                    allGeneratedKeys.addAll(batchResult.getGeneratedKeys());
                }
            }
            
            return new BatchInsertResult(totalAffectedRows, allGeneratedKeys);
            
        } catch (Exception e) {
            return BatchInsertResult.failure(e.getMessage());
        }
    }
    
    // ==================== 核心批处理方法 ====================
    
    /**
     * 批处理核心方法
     */
    private int processBatch(LoaderConfig config, List<Map<String, Object>> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        
        String operationType = config.getLoadType().getValue().toUpperCase();
        
        switch (operationType) {
            case "INSERT":
                return processBatchInsert(config, batch);
            case "UPDATE":
                return processBatchUpdate(config, batch);
            case "UPSERT":
                return processBatchUpsert(config, batch);
            case "DELETE":
                return processBatchDelete(config, batch);
            default:
                throw new IllegalArgumentException("Unsupported operation type: " + operationType);
        }
    }
    
    /**
     * 批量插入
     */
    private int processBatchInsert(LoaderConfig config, List<Map<String, Object>> batch) {
        SqlBuilder sqlBuilder = createSqlBuilder(config);
        String sql = sqlBuilder.buildBatchInsertSql(batch.size());
        
        List<Map<String, Object>> paramsList = buildParametersList(config, batch);
        return dataLoaderMapper.executeSql(sql, Map.of("paramsList", paramsList));
    }
    
    /**
     * 批量更新
     */
    private int processBatchUpdate(LoaderConfig config, List<Map<String, Object>> batch) {
        SqlBuilder sqlBuilder = createSqlBuilder(config);
        String sql = sqlBuilder.buildBatchUpdateSql(batch.size());
        
        List<Map<String, Object>> paramsList = buildParametersList(config, batch);
        return dataLoaderMapper.executeSql(sql, Map.of("paramsList", paramsList));
    }
    
    /**
     * 批量UPSERT
     */
    private int processBatchUpsert(LoaderConfig config, List<Map<String, Object>> batch) {
        try {
            return processBatchInsert(config, batch);
        } catch (Exception e) {
            // 如果插入失败，尝试更新
            return processBatchUpdate(config, batch);
        }
    }
    
    /**
     * 批量删除
     */
    private int processBatchDelete(LoaderConfig config, List<Map<String, Object>> batch) {
        SqlBuilder sqlBuilder = createSqlBuilder(config);
        
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(config.getTableName()).append(" WHERE ");
        
        List<String> whereInClauses = new ArrayList<>();
        for (String pk : sqlBuilder.getPrimaryKeys()) {
            StringBuilder inClause = new StringBuilder();
            inClause.append(pk).append(" IN (");
            List<String> pkValues = new ArrayList<>();
            for (int i = 0; i < batch.size(); i++) {
                pkValues.add("#{paramsList[" + i + "]." + pk + "}");
            }
            inClause.append(String.join(", ", pkValues)).append(")");
            whereInClauses.add(inClause.toString());
        }
        sql.append(String.join(" AND ", whereInClauses));
        
        List<Map<String, Object>> paramsList = buildParametersList(config, batch);
        return dataLoaderMapper.executeSql(sql.toString(), Map.of("paramsList", paramsList));
    }
    
    // ==================== 工具方法 ====================
    
    /**
     * 创建SqlBuilder
     */
    private SqlBuilder createSqlBuilder(LoaderConfig config) {
        SqlBuilder sqlBuilder = new SqlBuilder(config.getTableName());
        
        List<String> fields = extractFields(config);
        List<String> primaryKeys = extractPrimaryKeys(config);
        
        sqlBuilder.setFields(fields);
        sqlBuilder.setPrimaryKeys(primaryKeys);
        
        return sqlBuilder;
    }
    
    /**
     * 提取字段列表
     */
    private List<String> extractFields(LoaderConfig config) {
        List<String> fields = new ArrayList<>();
        for (FieldMapping mapping : config.getFieldMappings()) {
            fields.add(mapping.getTargetField());
        }
        return fields;
    }
    
    /**
     * 提取主键列表
     */
    private List<String> extractPrimaryKeys(LoaderConfig config) {
        List<String> primaryKeys = new ArrayList<>();
        for (FieldMapping mapping : config.getFieldMappings()) {
            if (mapping.isPrimaryKey()) {
                primaryKeys.add(mapping.getTargetField());
            }
        }
        return primaryKeys;
    }
    
    /**
     * 构建参数映射
     */
    private Map<String, Object> buildParameters(LoaderConfig config, Map<String, Object> row) {
        Map<String, Object> params = new HashMap<>();
        
        for (FieldMapping mapping : config.getFieldMappings()) {
            Object value = row.get(mapping.getSourceField());
            if (value == null && mapping.getDefaultValue() != null) {
                value = mapping.getDefaultValue();
            }
            params.put(mapping.getTargetField(), value);
        }
        
        return params;
    }
    
    /**
     * 构建参数列表
     */
    private List<Map<String, Object>> buildParametersList(LoaderConfig config, List<Map<String, Object>> batch) {
        List<Map<String, Object>> paramsList = new ArrayList<>();
        for (Map<String, Object> row : batch) {
            paramsList.add(buildParameters(config, row));
        }
        return paramsList;
    }
    
    /**
     * 从处理后的数据中提取生成的主键
     */
    private List<Long> extractGeneratedKeys(List<Map<String, Object>> processedDataList) {
        List<Long> keys = new ArrayList<>();
        for (Map<String, Object> row : processedDataList) {
            Object id = row.get("id"); // 假设主键字段名为id
            if (id != null) {
                if (id instanceof Number) {
                    keys.add(((Number) id).longValue());
                } else {
                    try {
                        keys.add(Long.parseLong(id.toString()));
                    } catch (NumberFormatException e) {
                        keys.add(null);
                    }
                }
            } else {
                keys.add(null);
            }
        }
        return keys;
    }
    
    // ==================== 校验方法 ====================
    
    private void validateConfig(LoaderConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("LoaderConfig cannot be null");
        }
        if (config.getTableName() == null || config.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (config.getLoadType() == null) {
            throw new IllegalArgumentException("Load type cannot be null");
        }
        if (config.getFieldMappings() == null || config.getFieldMappings().isEmpty()) {
            throw new IllegalArgumentException("Field mappings cannot be null or empty");
        }
    }
    
    private void validateData(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            throw new IllegalArgumentException("Data list cannot be null");
        }
    }
    
    // ==================== 内部类 ====================
    
    /**
     * 加载结果类
     */
    public static class LoadResult {
        private String configId;
        private int totalRecords;
        private int successRecords;
        private int failedRecords;
        private long startTime;
        private long endTime;
        private List<String> errors = new ArrayList<>();
        
        // Getters and Setters
        public String getConfigId() { return configId; }
        public void setConfigId(String configId) { this.configId = configId; }
        
        public int getTotalRecords() { return totalRecords; }
        public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }
        
        public int getSuccessRecords() { return successRecords; }
        public void setSuccessRecords(int successRecords) { this.successRecords = successRecords; }
        
        public int getFailedRecords() { return failedRecords; }
        public void setFailedRecords(int failedRecords) { this.failedRecords = failedRecords; }
        
        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }
        
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }
        
        public List<String> getErrors() { return errors; }
        public void setErrors(List<String> errors) { this.errors = errors; }
        
        public void addError(String error) { this.errors.add(error); }
        
        public boolean isSuccess() { return failedRecords == 0 && errors.isEmpty(); }
        
        public long getDuration() { return endTime - startTime; }
    }
    
    /**
     * 批量插入结果类
     */
    public static class BatchInsertResult {
        private boolean success = true;
        private int affectedRows;
        private List<Long> generatedKeys;
        private String errorMessage;
        
        public BatchInsertResult() {}
        
        public BatchInsertResult(int affectedRows, List<Long> generatedKeys) {
            this.success = true;
            this.affectedRows = affectedRows;
            this.generatedKeys = generatedKeys;
        }
        
        public static BatchInsertResult failure(String errorMessage) {
            BatchInsertResult result = new BatchInsertResult();
            result.success = false;
            result.errorMessage = errorMessage;
            return result;
        }
        
        // Getters and Setters
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public int getAffectedRows() { return affectedRows; }
        public void setAffectedRows(int affectedRows) { this.affectedRows = affectedRows; }
        
        public List<Long> getGeneratedKeys() { return generatedKeys; }
        public void setGeneratedKeys(List<Long> generatedKeys) { this.generatedKeys = generatedKeys; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }
}
