package com.example.demo.transform.service;

import com.example.demo.transform.model.TransformConfig;

import java.util.List;
import java.util.Map;

/**
 * 转换服务接口
 */
public interface TransformService {
    /**
     * 执行数据转换
     * @param sourceData 源数据
     * @param config 转换配置
     * @return 转换后的数据
     */
    List<Map<String, Object>> transform(List<Map<String, Object>> sourceData, TransformConfig config);
    
    /**
     * 保存转换配置
     * @param config 转换配置
     * @return 保存后的配置ID
     */
    String saveConfig(TransformConfig config);
    
    /**
     * 获取转换配置
     * @param configId 配置ID
     * @return 转换配置
     */
    TransformConfig getConfig(String configId);
    
    /**
     * 获取所有转换配置
     * @return 所有转换配置
     */
    List<TransformConfig> getAllConfigs();
}