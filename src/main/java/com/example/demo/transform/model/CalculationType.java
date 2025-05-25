package com.example.demo.transform.model;

/**
 * 计算类型枚举
 * 定义支持的各种转换计算类型
 */
public enum CalculationType {
    // 源值赋值
    SOURCE_VALUE,
    // 赋值常量
    CONSTANT_VALUE,
    // 赋值默认值
    DEFAULT_VALUE,
    // 定制函数
    CUSTOM_FUNCTION,
    // 四则运算
    ARITHMETIC_OPERATION
}