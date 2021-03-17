package com.codecubic.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class ESConfig implements Serializable {
    private String httpHostInfo;
    private Integer batch = 100;
    private Integer parallel = 10;
    private Long bufferWriteSize = 10L;
    private Integer connectTimeoutMillis = 30000;
    private Integer socketTimeoutMillis = 30000;
    private Integer connectionRequestTimeoutMillis = 2000;
    private Integer maxConnectPerRoute = 5;
    private Integer maxConnectTotal = 10;
    private Integer ioThreadCount = 2;
    private Long bufferFlushInterval = 2L;
    private Long backOffSec = 2L;
    private Integer backOffRetries = 3;
    private Integer reqFailRetryWaitSec = connectTimeoutMillis/3000;
    /**
     * es索引写入数据，等待关闭秒数
     */
    private long awaitCloseSec = 5L;

    private String indexSchemaTemplate;

    /**
     * 建议不要小于500毫秒
     */
    private Integer bufferFlushWaitMill = 1000;

    private Integer reqWriteWaitMill = 10000;


}
