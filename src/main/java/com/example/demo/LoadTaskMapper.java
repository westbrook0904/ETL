public interface LoadTaskMapper {
    // 查询加载任务
    LoadTask getTaskById(Long id);
    LoadTask getTaskByCode(String taskCode);
    List<LoadTask> getAllTasks();
    
    // 查询字段映射
    List<FieldMapping> getFieldMappingsByTaskId(Long taskId);
    
    // 查询条件配置
    List<ConditionConfig> getConditionsByTaskId(Long taskId);
    
    // 执行动态SQL
    int executeInsertSql(@Param("sql") String sql, @Param("params") Map<String, Object> params);
    int executeUpdateSql(@Param("sql") String sql, @Param("params") Map<String, Object> params);
    int executeDeleteSql(@Param("sql") String sql, @Param("params") Map<String, Object> params);
}
