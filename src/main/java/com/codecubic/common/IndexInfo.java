package com.codecubic.common;


import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class IndexInfo implements Serializable {
    private String name;
    private String type;
    private List<String> alias;
    private PropertiesInfo propInfo;
}
