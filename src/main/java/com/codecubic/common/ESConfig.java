package com.codecubic.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class ESConfig implements Serializable {
    private String httpHostInfo;
    private Integer batch = 100;
    private Integer parallel = 2;
    private Long bufferWriteSize = 10L;
    private Integer connectTimeoutMillis = 1000;
    private Integer socketTimeoutMillis = 30000;
    private Integer connectionRequestTimeoutMillis = 500;
    private Integer maxConnectPerRoute = 10;
    private Integer maxConnectTotal = 30;
    private Integer ioThreadCount;
    /**
     * es索引写入数据，等待关闭秒数
     */
    private long awaitCloseSec = 2L;

    private String indexSchemaTemplate;

}
