package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 默认值赋值计算器
 * 当源字段值为null时，使用默认值
 */
@Component
public class DefaultValueCalculator implements ValueCalculator {
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        Object sourceValue = sourceData.get(fieldMapping.getSourceField());
        return sourceValue != null ? sourceValue : fieldMapping.getCalculationParam();
    }

    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.DEFAULT_VALUE.equals(fieldMapping.getCalculationType());
    }
}