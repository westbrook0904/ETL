package com.example.demo.transform.expression;

import java.util.Map;

/**
 * 表达式接口
 * 所有类型的表达式节点都实现此接口
 */
public interface Expression {
    /**
     * 计算表达式的值
     * @param context 计算上下文，包含字段值
     * @return 计算结果
     */
    Object evaluate(Map<String, Object> context);
}