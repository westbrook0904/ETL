package com.example.demo.transform.expression;

/**
 * 操作符枚举
 * 定义支持的四则运算操作符
 */
public enum Operator {
    ADD('+', 1),       // 加法
    SUBTRACT('-', 1),  // 减法
    MULTIPLY('*', 2),  // 乘法
    DIVIDE('/', 2);    // 除法
    
    private final char symbol;
    private final int precedence; // 优先级，数字越大优先级越高
    
    Operator(char symbol, int precedence) {
        this.symbol = symbol;
        this.precedence = precedence;
    }
    
    public char getSymbol() {
        return symbol;
    }
    
    public int getPrecedence() {
        return precedence;
    }
    
    public static Operator fromSymbol(char symbol) {
        for (Operator op : values()) {
            if (op.symbol == symbol) {
                return op;
            }
        }
        throw new IllegalArgumentException("不支持的操作符: " + symbol);
    }
}