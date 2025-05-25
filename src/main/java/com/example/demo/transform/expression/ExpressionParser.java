package com.example.demo.transform.expression;

import java.util.ArrayDeque;
 import java.util.Deque;

/**
 * 表达式解析器
 * 将字符串表达式解析为表达式树
 */
public class ExpressionParser {
    
    /**
     * 解析表达式字符串
     * @param expression 表达式字符串，如 "(field1 + field2) * field3"
     * @return 解析后的表达式树
     */
    public Expression parse(String expression) {
        // 移除所有空格
        expression = expression.replaceAll("\\s+", "");
        return parseExpression(expression);
    }
    
    private Expression parseExpression(String expression) {
        if (expression == null || expression.isEmpty()) {
            throw new IllegalArgumentException("表达式不能为空");
        }
        
        // 处理括号
        if (expression.startsWith("(") && expression.endsWith(")")) {
            // 检查是否真的需要去掉这对括号
            if (isMatchingParentheses(expression)) {
                return parseExpression(expression.substring(1, expression.length() - 1));
            }
        }
        
        // 查找优先级最低的操作符
        int pos = findLowestPrecedenceOperator(expression);
        if (pos > 0) {
            String leftExpr = expression.substring(0, pos);
            String rightExpr = expression.substring(pos + 1);
            Operator operator = Operator.fromSymbol(expression.charAt(pos));
            
            Expression left = parseExpression(leftExpr);
            Expression right = parseExpression(rightExpr);
            
            return new BinaryExpression(left, right, operator);
        }
        
        // 如果没有操作符，可能是字段引用或常量
        if (expression.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
            // 字段引用
            return new FieldExpression(expression);
        } else {
            // 尝试解析为数字常量
            try {
                return new ConstantExpression(Double.parseDouble(expression));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("无法解析表达式: " + expression);
            }
        }
    }
    
    private boolean isMatchingParentheses(String expression) {
        int count = 0;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (c == '(') {
                count++;
            } else if (c == ')') {
                count--;
            }
            // 如果在中间某个位置括号已经匹配完，说明最外层的括号不是一对
            if (count == 0 && i < expression.length() - 1) {
                return false;
            }
        }
        return count == 0;
    }
    
    private int findLowestPrecedenceOperator(String expression) {
        int minPrecedence = Integer.MAX_VALUE;
        int pos = -1;
        int parenthesesCount = 0;
        
        for (int i = expression.length() - 1; i >= 0; i--) {
            char c = expression.charAt(i);
            
            if (c == ')') {
                parenthesesCount++;
            } else if (c == '(') {
                parenthesesCount--;
            } else if (parenthesesCount == 0) {
                // 只在不在括号内的情况下查找操作符
                if ((c == '+' || c == '-' || c == '*' || c == '/')) {
                    Operator op = Operator.fromSymbol(c);
                    // 从右向左扫描，优先选择同等优先级中最右边的操作符
                    if (op.getPrecedence() <= minPrecedence) {
                        minPrecedence = op.getPrecedence();
                        pos = i;
                    }
                }
            }
        }
        
        return pos;
    }
}