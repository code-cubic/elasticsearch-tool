package com.codecubic.common;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class PropertiesInfo implements Serializable {
    @Getter
    private List<FieldInfo> fields = new ArrayList();

    public void addField(FieldInfo f) {
        this.fields.add(f);
    }

    public void addField(String name, String type) {
        this.fields.add(new FieldInfo(name, type));
    }

    public void addFields(List<FieldInfo> fields) {
        this.fields.addAll(fields);
    }
}
