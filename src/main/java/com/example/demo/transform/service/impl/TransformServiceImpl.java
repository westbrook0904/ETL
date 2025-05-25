package com.example.demo.transform.service.impl;

import com.example.demo.transform.calculator.ValueCalculator;
import com.example.demo.transform.model.FieldMapping;
import com.example.demo.transform.model.TransformConfig;
import com.example.demo.transform.service.TransformService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 转换服务实现类
 */
@Service
public class TransformServiceImpl implements TransformService {
    
    private final List<ValueCalculator> calculators;
    private final Map<String, TransformConfig> configStore = new HashMap<>();
    
    @Autowired
    public TransformServiceImpl(List<ValueCalculator> calculators) {
        this.calculators = calculators;
    }
    
    @Override
    public List<Map<String, Object>> transform(List<Map<String, Object>> sourceData, TransformConfig config) {
        List<Map<String, Object>> result = new ArrayList<>(sourceData.size());
        
        for (Map<String, Object> sourceItem : sourceData) {
            Map<String, Object> targetItem = new HashMap<>();
            
            for (FieldMapping fieldMapping : config.getFieldMappings()) {
                Object value = calculateValue(sourceItem, fieldMapping);
                targetItem.put(fieldMapping.getTargetField(), value);
            }
            
            result.add(targetItem);
        }
        
        return result;
    }
    
    private Object calculateValue(Map<String, Object> sourceItem, FieldMapping fieldMapping) {
        for (ValueCalculator calculator : calculators) {
            if (calculator.supports(fieldMapping)) {
                return calculator.calculate(sourceItem, fieldMapping);
            }
        }
        
        throw new IllegalArgumentException("No calculator found for field mapping: " + fieldMapping);
    }
    
    @Override
    public String saveConfig(TransformConfig config) {
        String configId = String.valueOf(System.currentTimeMillis());
        configStore.put(configId, config);
        return configId;
    }
    
    @Override
    public TransformConfig getConfig(String configId) {
        return configStore.get(configId);
    }
    
    @Override
    public List<TransformConfig> getAllConfigs() {
        return new ArrayList<>(configStore.values());
    }
}