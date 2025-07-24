package com.dataloader.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;
import java.util.Map;

@Mapper
public interface DataLoaderMapper {
    
    /**
     * 执行动态SQL
     */
    int executeSql(@Param("sql") String sql, @Param("params") Map<String, Object> params);
    
    /**
     * 批量执行动态SQL
     */
    int[] executeBatchSql(@Param("sql") String sql, @Param("paramsList") List<Map<String, Object>> paramsList);
    
    /**
     * 批量插入并返回生成的主键
     */
    int batchInsertWithKeys(@Param("tableName") String tableName,
                           @Param("fields") List<String> fields,
                           @Param("dataList") List<Map<String, Object>> dataList);
    
    /**
     * 批量插入（使用动态SQL）
     */
    int batchInsertDynamic(@Param("sql") String sql, 
                          @Param("dataList") List<Map<String, Object>> dataList);
    
    /**
     * 批量更新
     */
    int batchUpdate(@Param("tableName") String tableName,
                   @Param("fields") List<String> fields,
                   @Param("primaryKeys") List<String> primaryKeys,
                   @Param("dataList") List<Map<String, Object>> dataList);
    
    /**
     * 批量UPSERT
     */
    int batchUpsert(@Param("tableName") String tableName,
                   @Param("fields") List<String> fields,
                   @Param("primaryKeys") List<String> primaryKeys,
                   @Param("dataList") List<Map<String, Object>> dataList);
    
    /**
     * 批量删除
     */
    int batchDelete(@Param("tableName") String tableName,
                   @Param("primaryKeys") List<String> primaryKeys,
                   @Param("dataList") List<Map<String, Object>> dataList);
}
