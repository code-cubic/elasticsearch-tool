package com.codecubic.common;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Data
public class FieldInfo implements Serializable {
    private String name;
    private String type;
    /**
     * 用于设置Object 、Nested类型的子类型
     */
    private PropertiesInfo propInfo;

    public FieldInfo() {

    }

    public FieldInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public void addFields(FieldInfo... fields) {
        if (propInfo == null) {
            propInfo = new PropertiesInfo();
        }
        propInfo.addFields(fields);
    }

    public Map<String, Map<String, String>> getInnerFieldTypeMap() {
        Map<String, Map<String, String>> properties = new HashMap<>(propInfo.getFields().size());
        for (FieldInfo f : propInfo.getFields()) {
            Map<String, String> map = new HashMap<>(1);
            map.put("type", f.getType());
            properties.put(f.getName(), map);
        }
        return properties;
    }

    public JSONObject getInnerFieldTypeJSON() {
        JSONObject props = new JSONObject(propInfo.getFields().size());
        for (FieldInfo f : propInfo.getFields()) {
            JSONObject json = new JSONObject(1);
            json.put("type", f.getType());
            props.put(f.getName(), json);
        }
        return props;
    }
}
