package com.example.demo.transform.expression;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 二元操作表达式
 * 表示两个表达式之间的四则运算
 */
@Data
@AllArgsConstructor
public class BinaryExpression implements Expression {
    private Expression left;
    private Expression right;
    private Operator operator;
    
    @Override
    public Object evaluate(Map<String, Object> context) {
        Object leftValue = left.evaluate(context);
        Object rightValue = right.evaluate(context);
        
        // 转换为BigDecimal进行精确计算
        BigDecimal leftDecimal = toBigDecimal(leftValue);
        BigDecimal rightDecimal = toBigDecimal(rightValue);
        
        switch (operator) {
            case ADD:
                return leftDecimal.add(rightDecimal);
            case SUBTRACT:
                return leftDecimal.subtract(rightDecimal);
            case MULTIPLY:
                return leftDecimal.multiply(rightDecimal);
            case DIVIDE:
                // 处理除零情况
                if (rightDecimal.compareTo(BigDecimal.ZERO) == 0) {
                    throw new ArithmeticException("除数不能为零");
                }
                return leftDecimal.divide(rightDecimal, 10, BigDecimal.ROUND_HALF_UP);
            default:
                throw new UnsupportedOperationException("不支持的操作符: " + operator);
        }
    }
    
    private BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return BigDecimal.ZERO;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无法将值转换为数字: " + value);
        }
    }
}