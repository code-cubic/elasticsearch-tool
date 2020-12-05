package com.codecubic.common;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DocData {
    private String id;
    private long version;
    private List<FieldData> fieldDatas = new ArrayList();


    public void addField(FieldData f) {
        this.fieldDatas.add(f);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> jsonMap = new HashMap<>(this.fieldDatas.size());
        for (FieldData f : fieldDatas) {
            jsonMap.put(f.getName(), f.getVal());
        }
        return jsonMap;
    }

    public int getValInt(String fName) {
        for (FieldData f : fieldDatas) {
            if (f.getName().equals(fName)) {
                return (Integer) f.getVal();
            }
        }
        return Integer.MIN_VALUE;
    }

    public String getValStr(String fName) {
        for (FieldData f : fieldDatas) {
            if (f.getName().equals(fName)) {
                return f.getVal() != null ? f.getVal().toString() : null;
            }
        }
        return null;
    }

    public double getValDouble(String fName) {
        for (FieldData f : fieldDatas) {
            if (f.getName().equals(fName)) {
                return (Double) f.getVal();
            }
        }
        return Double.MIN_VALUE;
    }
}
