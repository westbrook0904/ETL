package com.example.demo.transform.function;

import com.example.demo.transform.model.FunctionConfig;

import java.util.Map;

/**
 * 函数执行器接口
 * 负责执行各种自定义函数
 */
public interface FunctionExecutor {
    /**
     * 执行函数
     * @param functionConfig 函数配置
     * @param context 执行上下文
     * @return 函数执行结果
     */
    Object execute(FunctionConfig functionConfig, Map<String, Object> context);
}