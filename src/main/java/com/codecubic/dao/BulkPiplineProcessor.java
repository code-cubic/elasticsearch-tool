package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.ESConfig;
import com.codecubic.exception.BulkProcessorInitExcp;
import com.codecubic.util.TimeUtil;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class BulkPiplineProcessor implements Closeable {
    private RestHighLevelClient client;
    private ESConfig esConfig;
    private final List<DocWriteRequest> lazyQueue = new LinkedList<>();
    private volatile BulkProcessor bulkProcessor;

    public BulkPiplineProcessor(RestHighLevelClient client, ESConfig esConf) throws BulkProcessorInitExcp {
        this.client = client;
        this.esConfig = esConf;
        initBulkProcessor();
    }

    public void flush() {
        do {
            this.bulkProcessor.flush();
            retryWriteAll();
            TimeUtil.sleepSec(this.esConfig.getAwaitCloseSec().intValue());
        }
        while (!this.lazyQueue.isEmpty());
    }


    private static class RetryFailureListener implements BulkProcessor.Listener {
        private final List<DocWriteRequest> lazyQueue;

        public RetryFailureListener(List<DocWriteRequest> lazyQueue) {
            this.lazyQueue = lazyQueue;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            List<String> ids = request.requests().stream().map(e -> e.id()).collect(Collectors.toList());
            log.debug("ids:{}", String.join(",", ids));
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            for (BulkItemResponse bulkItemResponse : response) {
                int status = bulkItemResponse.status().getStatus();
                if (bulkItemResponse.isFailed()) {
                    DocWriteRequest<?> req = request.requests().stream().filter(e -> e.id().equals(bulkItemResponse.getFailure().getId())).findFirst().get();
                    if (429 == status) {
                        this.lazyQueue.add(req);
                    }
                    log.error("status:{},doc id:{}", status, req.id());
                    continue;
                }
            }

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            log.error("", failure);
        }
    }

    private synchronized void initBulkProcessor() throws BulkProcessorInitExcp {
        try {
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (rq, listener) -> this.client.bulkAsync(rq, RequestOptions.DEFAULT, listener);
            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, new RetryFailureListener(this.lazyQueue));
            builder.setBulkActions(this.esConfig.getBatch());
            builder.setBulkSize(new ByteSizeValue(this.esConfig.getBufferWriteSize(), ByteSizeUnit.MB));
            builder.setConcurrentRequests(this.esConfig.getParallel());
//            builder.setFlushInterval(TimeValue.timeValueSeconds(this.esConfig.getBufferFlushInterval()));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(this.esConfig.getBackOffSec()), this.esConfig.getBackOffRetries()));
            this.bulkProcessor = builder.build();
        } catch (Exception e) {
            throw new BulkProcessorInitExcp(e);
        }
    }

    /**
     * @param indexName
     * @param docType
     * @param docs
     * @return true: submit suss
     */
    public boolean asyncBulkUpsert(String indexName, String docType, List<DocData> docs) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        Preconditions.checkNotNull(docs, "docs can not be null");
        try {
            if (!this.lazyQueue.isEmpty()) {
                log.warn("lazyExe,sleep:{}", this.esConfig.getReqWriteWaitMill());
                TimeUtil.sleepMill(this.esConfig.getReqWriteWaitMill());
            }
            for (DocData doc : docs) {
                Map<String, Object> objectMap = doc.toMap();
                UpdateRequest request = new UpdateRequest(indexName, docType, doc.getId())
                        .upsert(objectMap).doc(objectMap);
                request.retryOnConflict(10);
                request.waitForActiveShards(1);
                request.timeout(TimeValue.timeValueMillis(this.esConfig.getReqWriteWaitMill()));
                this.bulkProcessor.add(request);
            }
            retryWrite();
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
        return true;
    }

    /**
     * @param indexName
     * @param docType
     * @param docIds
     * @return true:submit suss
     */
    public boolean asyBulkDelDoc(String indexName, String docType, Collection<String> docIds) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        Preconditions.checkNotNull(docIds, "docIds can not be null");
        try {
            for (String id : docIds) {
                DeleteRequest request = new DeleteRequest(indexName, docType, id);
                request.waitForActiveShards(1);
                this.bulkProcessor.add(request);
            }
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
        return true;
    }

    private void retryWrite() {
        if (!this.lazyQueue.isEmpty()) {
            log.warn("lazyExe,sleep:{}", this.esConfig.getReqWriteWaitMill());
            TimeUtil.sleepMill(this.esConfig.getReqWriteWaitMill());
        }
        int i = 0;
        Iterator<DocWriteRequest> item = lazyQueue.iterator();
        while (item.hasNext() && i < 200) {
            this.bulkProcessor.add(this.lazyQueue.get(0));
            this.lazyQueue.remove(0);
            i++;
        }
    }

    private void retryWriteAll() {
        if (!this.lazyQueue.isEmpty()) {
            log.warn("lazyExe,sleep:{}", this.esConfig.getReqWriteWaitMill());
            TimeUtil.sleepMill(this.esConfig.getReqWriteWaitMill());
        }
        Iterator<DocWriteRequest> item = lazyQueue.iterator();
        while (item.hasNext()) {
            this.bulkProcessor.add(this.lazyQueue.get(0));
            this.lazyQueue.remove(0);
        }
    }


    @Override
    public void close() {
        this.flush();
        if (this.bulkProcessor != null) {
            try {
                this.bulkProcessor.awaitClose(this.esConfig.getAwaitCloseSec(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
