package com.example.demo.transform.calculator.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.function.FunctionExecutor;
import com.example.demo.transform.model.CalculationType;
import com.example.demo.transform.model.FieldMapping;
import com.example.demo.transform.model.FunctionConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 自定义函数计算器
 * 执行自定义函数，如日期格式化、工号转换等
 */
@Component
public class CustomFunctionCalculator implements ValueCalculator {

    @Autowired
    private FunctionExecutor functionExecutor;
    
    @Override
    public Object calculate(Map<String, Object> sourceData, FieldMapping fieldMapping) {
        FunctionConfig functionConfig = (FunctionConfig) fieldMapping.getCalculationParam();
        if (functionConfig == null) {
            throw new IllegalArgumentException("函数配置不能为空");
        }
        
        return functionExecutor.execute(functionConfig, sourceData);
    }
    
    @Override
    public boolean supports(FieldMapping fieldMapping) {
        return CalculationType.CUSTOM_FUNCTION.equals(fieldMapping.getCalculationType());
    }
}