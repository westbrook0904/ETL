package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.expression.Expression;
import com.example.demo.transform.expression.ExpressionParser;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 四则运算计算器
 * 支持复杂的四则运算表达式，如(field1 + field2) * field3
 */
@Component
public class ArithmeticOperationCalculator implements ValueCalculator {
    
    private final ExpressionParser expressionParser = new ExpressionParser();
    
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        String expressionStr = (String) fieldMapping.getCalculationParam();
        if (expressionStr == null || expressionStr.trim().isEmpty()) {
            throw new IllegalArgumentException("四则运算表达式不能为空");
        }
        
        // 解析表达式
        Expression expression = expressionParser.parse(expressionStr);
        
        // 计算表达式的值
        return expression.evaluate(sourceData);
    }
    
    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.ARITHMETIC_OPERATION.equals(fieldMapping.getCalculationType());
    }
}