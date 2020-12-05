package com.codecubic.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class FieldData implements Serializable {
    private String name;
    private Object val;
}
