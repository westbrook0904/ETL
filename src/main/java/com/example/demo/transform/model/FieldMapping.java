package com.example.demo.transform.model;

import lombok.Data;

/**
 * 字段映射配置类
 * 定义源字段到目标字段的映射关系
 */
@Data
public class FieldMapping {
    // 源字段名
    private String sourceField;
    // 源字段类型
    private String sourceType;
    // 目标字段名
    private String targetField;
    // 目标字段类型
    private String targetType;
    // 计算类型
    private CalculationType calculationType;
    // 计算参数，根据不同的计算类型有不同的参数
    private Object calculationParam;
}