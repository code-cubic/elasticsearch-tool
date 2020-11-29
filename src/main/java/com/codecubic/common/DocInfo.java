package com.codecubic.common;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DocInfo {
    private String id;
    private List<FieldInfo> fieldInfoList = new ArrayList();

    public void addField(FieldInfo f) {
        this.fieldInfoList.add(f);
    }
}
