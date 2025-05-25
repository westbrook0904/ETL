package com.example.demo.transform.model;

import lombok.Data;

import java.util.List;

/**
 * 转换配置类
 * 包含所有字段映射配置
 */
@Data
public class TransformConfig {
    // 配置名称
    private String name;
    // 配置描述
    private String description;
    // 字段映射列表
    private List<FieldMapping> fieldMappings;
}