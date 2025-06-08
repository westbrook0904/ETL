package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 四则运算计算器
 * 支持复杂的四则运算表达式，如(field1 + field2) * field3
 * 使用逆波兰表达式（后缀表达式）实现
 */
@Component
public class ArithmeticOperationCalculator implements ValueCalculator {
    
    /**
     * 使用逆波兰表达式计算四则运算
     * 
     * @param expressionStr 表达式字符串，如 "(field1 + field2) * field3"
     * @param paramMap 参数映射，字段名到字段值的映射
     * @param precision 结果精度，小数点后位数
     * @return 计算结果字符串
     */
    public String calculateWithRPN(String expressionStr, Map<String, String> paramMap, int precision) {
        if (expressionStr == null || expressionStr.trim().isEmpty()) {
            throw new IllegalArgumentException("四则运算表达式不能为空");
        }
        
        // 1. 将中缀表达式转换为后缀表达式（逆波兰表达式）
        List<String> rpnTokens = infixToRPN(expressionStr);
        
        // 2. 计算后缀表达式
        BigDecimal result = evaluateRPN(rpnTokens, paramMap);
        
        // 3. 根据精度格式化结果
        return result.setScale(precision, RoundingMode.HALF_UP).toPlainString();
    }
    
    /**
     * 将中缀表达式转换为后缀表达式（逆波兰表达式）
     */
    private List<String> infixToRPN(String expression) {
        // 移除所有空格
        expression = expression.replaceAll("\\s+", "");
        
        List<String> tokens = tokenize(expression);
        List<String> output = new ArrayList<>();
        Deque<String> operatorStack = new ArrayDeque<>();
        
        for (String token : tokens) {
            if (isNumber(token) || isVariable(token)) {
                // 数字或变量直接输出
                output.add(token);
            } else if (isOperator(token)) {
                // 处理操作符
                while (!operatorStack.isEmpty() && 
                       !operatorStack.peek().equals("(") && 
                       (getPrecedence(operatorStack.peek()) >= getPrecedence(token))) {
                    output.add(operatorStack.pop());
                }
                operatorStack.push(token);
            } else if (token.equals("(")) {
                // 左括号入栈
                operatorStack.push(token);
            } else if (token.equals(")")) {
                // 处理右括号
                while (!operatorStack.isEmpty() && !operatorStack.peek().equals("(")) {
                    output.add(operatorStack.pop());
                }
                if (!operatorStack.isEmpty() && operatorStack.peek().equals("(")) {
                    operatorStack.pop(); // 弹出左括号
                } else {
                    throw new IllegalArgumentException("括号不匹配");
                }
            }
        }
        
        // 将栈中剩余的操作符加入输出
        while (!operatorStack.isEmpty()) {
            String op = operatorStack.pop();
            if (op.equals("(")) {
                throw new IllegalArgumentException("括号不匹配");
            }
            output.add(op);
        }
        
        return output;
    }
    
    /**
     * 计算后缀表达式（逆波兰表达式）
     */
    private BigDecimal evaluateRPN(List<String> tokens, Map<String, String> paramMap) {
        Deque<BigDecimal> stack = new ArrayDeque<>();
        
        for (String token : tokens) {
            if (isNumber(token)) {
                // 数字直接入栈
                stack.push(new BigDecimal(token));
            } else if (isVariable(token)) {
                // 变量替换为对应的值
                String value = paramMap.get(token);
                if (value == null) {
                    throw new IllegalArgumentException("未找到变量的值: " + token);
                }
                try {
                    stack.push(new BigDecimal(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("变量值不是有效的数字: " + token + " = " + value);
                }
            } else if (isOperator(token)) {
                // 操作符计算
                if (stack.size() < 2) {
                    throw new IllegalArgumentException("表达式格式错误");
                }
                
                BigDecimal b = stack.pop();
                BigDecimal a = stack.pop();
                
                switch (token) {
                    case "+":
                        stack.push(a.add(b));
                        break;
                    case "-":
                        stack.push(a.subtract(b));
                        break;
                    case "*":
                        stack.push(a.multiply(b));
                        break;
                    case "/":
                        if (b.compareTo(BigDecimal.ZERO) == 0) {
                            throw new ArithmeticException("除数不能为零");
                        }
                        // 除法使用高精度计算，最后再根据需要四舍五入
                        stack.push(a.divide(b, 20, RoundingMode.HALF_UP));
                        break;
                    default:
                        throw new UnsupportedOperationException("不支持的操作符: " + token);
                }
            }
        }
        
        if (stack.size() != 1) {
            throw new IllegalArgumentException("表达式格式错误");
        }
        
        return stack.pop();
    }
    
    /**
     * 将表达式分割为标记（数字、变量、操作符、括号）
     */
    private List<String> tokenize(String expression) {
        List<String> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            
            if (c == '+' || c == '-' || c == '*' || c == '/' || c == '(' || c == ')') {
                // 如果当前有累积的标记，先添加到结果中
                if (sb.length() > 0) {
                    tokens.add(sb.toString());
                    sb.setLength(0);
                }
                // 添加操作符或括号
                tokens.add(String.valueOf(c));
            } else {
                // 累积数字或变量名
                sb.append(c);
            }
        }
        
        // 添加最后一个标记
        if (sb.length() > 0) {
            tokens.add(sb.toString());
        }
        
        return tokens;
    }
    
    /**
     * 判断是否为数字
     */
    private boolean isNumber(String token) {
        try {
            new BigDecimal(token);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * 判断是否为变量
     * 修改后的逻辑：任何非数字的标记都视为变量
     */
    private boolean isVariable(String token) {
        // 不是数字且不是操作符和括号的都视为变量
        return !isNumber(token) && !isOperator(token) && !token.equals("(") && !token.equals(")");
    }
    
    private boolean isOperator(String token) {
        return token.equals("+") || token.equals("-") || token.equals("*") || token.equals("/");
    }
    
    private int getPrecedence(String operator) {
        switch (operator) {
            case "+":
            case "-":
                return 1;
            case "*":
            case "/":
                return 2;
            default:
                return 0;
        }
    }
    
    /**
     * 从表达式中提取字段名
     * 修改后的逻辑：提取所有非数字、非操作符、非括号的标记作为字段名
     * @param expressionStr 表达式字符串
     * @return 字段名集合
     */
    public Set<String> extractFieldNames(String expressionStr) {
        // 移除所有空格
        expressionStr = expressionStr.replaceAll("\\s+", "");
        
        Set<String> fieldNames = new HashSet<>();
        List<String> tokens = tokenize(expressionStr);
        
        for (String token : tokens) {
            if (!isNumber(token) && !isOperator(token) && !token.equals("(") && !token.equals(")")) {
                fieldNames.add(token);
            }
        }
        
        return fieldNames;
    }
    
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        String expressionStr = (String) fieldMapping.getCalculationParam();
        if (expressionStr == null || expressionStr.trim().isEmpty()) {
            throw new IllegalArgumentException("四则运算表达式不能为空");
        }
        
        // 提取表达式中的字段名
        Set<String> fieldNames = extractFieldNames(expressionStr);
        
        // 构建参数映射
        Map<String, String> paramMap = new HashMap<>();
        for (String fieldName : fieldNames) {
            Object value = sourceData.get(fieldName);
            if (value != null) {
                paramMap.put(fieldName, value.toString());
            } else {
                throw new IllegalArgumentException("未找到字段的值: " + fieldName);
            }
        }
        
        // 默认精度为2位小数
        int precision = 2;
        
        // 计算表达式的值
        return calculateWithRPN(expressionStr, paramMap, precision);
    }
    
    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.ARITHMETIC_OPERATION.equals(fieldMapping.getCalculationType());
    }
}
