package com.codecubic.common;

import com.codecubic.exception.ConfigErrExcp;
import lombok.Data;

import java.io.Serializable;

@Data
public class ESConfig implements Serializable {
    private String httpHostInfo;
    private Integer batch = 100;
    private Integer parallel = 2;
    private Long bufferWriteSize = 10L;
    private Integer connectTimeoutMillis = 30000;
    private Integer socketTimeoutMillis = 30000;
    private Integer connectionRequestTimeoutMillis = 2000;
    private Integer maxConnectPerRoute = 5;
    private Integer maxConnectTotal = 10;
    private Integer ioThreadCount = 2;
    /**
     * 不可设置，存在刷新bug，当bulk出现异常时，会很快耗尽并发信号量
     */
    private Long bufferFlushInterval = 3L;
    private Long backOffSec = 2L;
    private Integer backOffRetries = 3;
    private Integer reqFailRetryWaitSec = 15;
    /**
     * es索引写入数据，等待关闭秒数
     */
    private Long awaitCloseSec = 5L;

    private String indexSchemaTemplate;

    /**
     * 建议不要小于500毫秒
     */
    private Integer bufferFlushWaitMill = 500;

    private Integer reqWriteWaitMill = 1000;


    public void setParallel(Integer parallel) throws ConfigErrExcp {
        this.parallel = parallel;
        if (this.parallel < 2) {
            throw new ConfigErrExcp("parallel can not lower than 2!");
        }
    }

    public void setBufferFlushInterval(Long bufferFlushInterval) throws ConfigErrExcp {
        throw new ConfigErrExcp("can not setting bufferFlushInterval!");
    }
}
