[8 tools called]

设计一个管道执行态监控系统是一个非常好的实践，可以显著提高 ETL 任务的可观测性和运维效率。根据你的需求，我将从数据库设计、事件模型、监控集成和异步处理四个维度为你提供设计方案。

### 1. 数据库设计

首先，我们需要两张表：一张记录**管道执行实例**（Pipeline Instance），另一张记录**组件执行详情**（Component Execution Log）。

```sql
-- 管道执行实例表：记录单次管道运行的整体状态
CREATE TABLE pipeline_instance (
    instance_id VARCHAR(64) PRIMARY KEY, -- 执行态唯一ID
    pipeline_code VARCHAR(64) NOT NULL,  -- 关联的管道编码
    status VARCHAR(20) NOT NULL,        -- RUNNING, SUCCESS, FAILED
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    duration BIGINT,                    -- 耗时(ms)
    error_msg TEXT
);

-- 组件执行日志表：记录管道中每个组件的执行情况
CREATE TABLE component_execution_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    instance_id VARCHAR(64) NOT NULL,    -- 关联的管道实例ID
    component_code VARCHAR(64) NOT NULL, -- 组件编码
    component_type VARCHAR(32),          -- EXTRACTION, TRANSFORM, LOAD
    status VARCHAR(20) NOT NULL,        -- SUCCESS, FAILED
    input_count INT,                    -- 输入数据量
    output_count INT,                   -- 输出数据量
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    duration BIGINT,
    error_msg TEXT,
    INDEX idx_instance (instance_id)
);
```

### 2. 定义监控事件模型

利用 Spring 的 `ApplicationEvent` 机制来实现解耦。

```java
// 组件执行完成事件
public class ComponentFinishedEvent extends ApplicationEvent {
    private final String instanceId;
    private final String componentCode;
    private final String componentType;
    private final String status;
    private final int inputCount;
    private final int outputCount;
    private final long startTime;
    private final long endTime;
    private final String errorMsg;

    // Constructor, Getters...
}
```

### 3. 在 `BaseComponentOperation` 中集成监控逻辑

建议使用 **装饰器模式** 或者在执行引擎层统一处理，而不是让每个组件实现类都去写发事件的代码。

```java
public interface BaseComponentOperation {
    // 建议增加一个 Context 对象来传递 instanceId 等监控元数据
    List<List<Map<String, Object>>> execute(
        List<List<Map<String, Object>>> data, 
        ExecutionContext context
    );
}

// 监控装饰器示例
public class MonitoredComponentOperation implements BaseComponentOperation {
    private final BaseComponentOperation delegate;
    private final ApplicationEventPublisher publisher;

    @Override
    public List<List<Map<String, Object>>> execute(List<List<Map<String, Object>>> data, ExecutionContext context) {
        long start = System.currentTimeMillis();
        try {
            List<List<Map<String, Object>>> result = delegate.execute(data, context);
            
            // 发布成功事件
            publisher.publishEvent(new ComponentFinishedEvent(
                context.getInstanceId(), context.getComponentCode(), "SUCCESS", 
                count(data), count(result), start, System.currentTimeMillis(), null
            ));
            return result;
        } catch (Exception e) {
            // 发布失败事件
            publisher.publishEvent(new ComponentFinishedEvent(
                context.getInstanceId(), context.getComponentCode(), "FAILED", 
                count(data), 0, start, System.currentTimeMillis(), e.getMessage()
            ));
            throw e;
        }
    }
}
```

### 4. 异步监听与入库

使用 `@EventListener` 和 `@Async` 实现异步记录，确保监控逻辑不阻塞主执行流程。

```java
@Component
public class ExecutionMonitorListener {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Async("monitorExecutor") // 使用专门的线程池
    @EventListener
    public void handleComponentFinished(ComponentFinishedEvent event) {
        // 插入 component_execution_log 表
        String sql = "INSERT INTO component_execution_log (...) VALUES (...)";
        jdbcTemplate.update(sql, ...);
    }
}
```

### 5. 核心流程总结

1.  **准备阶段**：在管道启动前，生成一个唯一的 `instanceId`，并在 `pipeline_instance` 表中插入一条 `RUNNING` 记录。
2.  **执行阶段**：
    *   将 `instanceId` 放入 `ExecutionContext` 传递给每个组件。
    *   组件执行完成后（无论成功或失败），通过 `ApplicationEventPublisher` 发布事件。
3.  **监控阶段**：
    *   `ExecutionMonitorListener` 异步捕获事件。
    *   将执行明细写入数据库。
4.  **结束阶段**：管道所有组件执行完或异常中断时，更新 `pipeline_instance` 的最终状态。

这种设计的优点是：**侵入性低**（通过装饰器或 AOP 集成）、**性能影响小**（异步入库）、**扩展性强**（后续可以轻松增加钉钉报警、Prometheus 监控等订阅者）。
