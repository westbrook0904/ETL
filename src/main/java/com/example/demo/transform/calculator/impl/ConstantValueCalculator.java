package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 常量赋值计算器
 * 将配置的常量值赋给目标字段
 */
@Component
public class ConstantValueCalculator implements ValueCalculator {
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        return fieldMapping.getCalculationParam();
    }

    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.CONSTANT_VALUE.equals(fieldMapping.getCalculationType());
    }
}