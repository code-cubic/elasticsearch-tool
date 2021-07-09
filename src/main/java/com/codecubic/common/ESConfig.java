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
    private Integer connectTimeoutMillis = 60000;
    private Integer socketTimeoutMillis = 60000;
    private Integer connectionRequestTimeoutMillis = 3000;
    private Integer maxConnectPerRoute = 5;
    private Integer maxConnectTotal = 10;
    private Integer ioThreadCount = 2;
    /**
     * todo:
     * find issue : when bulk ayn processor failure on connect Exception,
     * datasource program will get stuck, if you set auto flush.
     * so we forbidden this param temporary!
     */
    private Long bufferFlushInterval = -1L;
    private Long backOffSec = 2L;
    private Integer backOffRetries = 3;
    private Long awaitCloseSec = 5L;
    private String indexSchemaTemplate;
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
