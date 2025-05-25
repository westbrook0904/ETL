package com.example.demo.transform.model;

import lombok.Data;

import java.util.Map;

/**
 * 函数配置类
 * 定义函数名称和参数
 */
@Data
public class FunctionConfig {
    // 函数名称
    private String functionName;
    // 函数参数
    private Map<String, Object> parameters;
}