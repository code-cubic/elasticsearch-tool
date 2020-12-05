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
    private PropertiesInfo propInfo;

    public String prop2JsonStr() {
        List<FieldInfo> fields = propInfo.getFields();
        JSONObject json = new JSONObject();
        fields.forEach(f -> {
            JSONObject e = new JSONObject();
            e.put("type", f.getType());
            json.put(f.getName(), e);
        });
        return json.toJSONString();
    }
}
