package com.example.demo.transform.expression;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * 常量表达式
 * 表示一个固定值
 */
@Data
@AllArgsConstructor
public class ConstantExpression implements Expression {
    private Object value;
    
    @Override
    public Object evaluate(Map<String, Object> context) {
        return value;
    }
}