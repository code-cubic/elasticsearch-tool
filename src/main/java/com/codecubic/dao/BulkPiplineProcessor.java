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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            Stream<DocWriteRequest<?>> reqStreams = request.requests().stream();
            for (BulkItemResponse bulkItemResponse : response) {
                int status = bulkItemResponse.status().getStatus();
                DocWriteRequest<?> req = reqStreams.filter(e -> e.id().equals(bulkItemResponse.getId())).findFirst().get();
                if (bulkItemResponse.isFailed()) {
                    if (429 == status) {
                        this.lazyQueue.add(req);
                    }
                    log.error("status:{},doc id:{}", status, req.id());
                    continue;
                }
                log.debug("status:{},doc id:{}", status, req.id());
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
            builder.setFlushInterval(TimeValue.timeValueSeconds(this.esConfig.getBufferFlushInterval()));
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
                TimeUnit.SECONDS.sleep(this.esConfig.getReqWriteWaitMill());
            }
            for (DocData doc : docs) {
                Map<String, Object> objectMap = doc.toMap();
                UpdateRequest request = new UpdateRequest(indexName, docType, doc.getId())
                        .upsert(objectMap).doc(objectMap);
                request.retryOnConflict(2);
                request.waitForActiveShards(1);
                request.timeout(TimeValue.timeValueSeconds(this.esConfig.getReqWriteWaitMill()));
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
        lazyQueue.forEach(this.bulkProcessor::add);
    }


    @Override
    public void close() {
        while (!lazyQueue.isEmpty()) {
            retryWrite();
        }
        if (this.bulkProcessor != null) {
            this.bulkProcessor.flush();
            try {
                this.bulkProcessor.awaitClose(this.esConfig.getAwaitCloseSec(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
