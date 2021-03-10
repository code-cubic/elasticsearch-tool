package com.codecubic.common;


import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class IndexInfo implements Serializable {
    private String name;
    private String type;
    private List<String> alias;
    private PropertiesInfo propInfo = new PropertiesInfo();

    public IndexInfo() {
    }

    public IndexInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String prop2JsonStr() {
        List<FieldInfo> fields = propInfo.getFields();
        JSONObject json = new JSONObject();
        fields.forEach(f -> {
            JSONObject e = new JSONObject();
            e.put("type", f.getType());
            if (f.getType().equalsIgnoreCase("nested") || f.getType().equalsIgnoreCase("object")) {
                e.put("properties",f.getInnerFieldTypeJSON());
            }
            json.put(f.getName(), e);
        });
        return json.toJSONString();
    }

    public void addFields(List<FieldInfo> fields) {
        propInfo.addFields(fields);
    }

    public void addFields(FieldInfo... fields) {
        propInfo.addFields(fields);
    }
}
