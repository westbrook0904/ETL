package com.example.demo.transform.expression;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * 字段引用表达式
 * 表示对数据源中某个字段的引用
 */
@Data
@AllArgsConstructor
public class FieldExpression implements Expression {
    private String fieldName;
    
    @Override
    public Object evaluate(Map<String, Object> context) {
        return context.get(fieldName);
    }
}