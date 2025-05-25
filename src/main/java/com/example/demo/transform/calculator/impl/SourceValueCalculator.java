package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 源值赋值计算器
 * 直接将源字段的值赋给目标字段
 */
@Component
public class SourceValueCalculator implements ValueCalculator {
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        return sourceData.get(fieldMapping.getSourceField());
    }

    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.SOURCE_VALUE.equals(fieldMapping.getCalculationType());
    }
}