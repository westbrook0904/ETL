package com.example.demo.transform.calculator;

import com.example.demo.transform.model.FieldMapping;

import java.util.Map;

/**
 * 值计算器接口
 * 所有具体的计算类型都需要实现这个接口
 */
public interface ValueCalculator {
    /**
     * 计算值
     * @param sourceData 源数据
     * @param fieldMapping 字段映射配置
     * @return 计算后的值
     */
    Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping);
    
    /**
     * 是否支持该计算类型
     * @param fieldMapping 字段映射配置
     * @return 是否支持
     */
    boolean supports(FieldMapping fieldMapping);
}