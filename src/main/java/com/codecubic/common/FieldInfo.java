package com.codecubic.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class FieldInfo implements Serializable {
    private String name;
    private String type;
    private Object val;
}
