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

}
