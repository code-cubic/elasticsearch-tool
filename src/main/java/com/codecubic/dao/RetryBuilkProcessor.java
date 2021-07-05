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
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class RetryBuilkProcessor implements Closeable {
    private RestHighLevelClient client;
    private ESConfig esConfig;
    private final List<DocWriteRequest> lazyQueue = new LinkedList<>();
    private final AtomicBoolean connS = new AtomicBoolean(true);
    /**
     * please do not use bulkProcessor.add,use this.addReq insteaded
     */
    private volatile BulkProcessor bulkProcessor;

    public RetryBuilkProcessor(RestHighLevelClient client, ESConfig esConf) throws BulkProcessorInitExcp {
        this.client = client;
        this.esConfig = esConf;
        this.bulkProcessor = buildProcessor();
    }

    public void flush() {
        do {
            if (this.connS.get()) {
                this.bulkProcessor.flush();
                retryWriteAll();
            }
            TimeUtil.sleepSec(this.esConfig.getAwaitCloseSec().intValue());
            log.debug("flush sleep:{}(s)", this.esConfig.getAwaitCloseSec());
        }
        while (!this.lazyQueue.isEmpty());
    }

    private void addReq(DocWriteRequest doc) {
        while (!this.connS.get()) {
            synchronized (this.bulkProcessor) {
                if (!this.connS.get()) {
                    try {
                        this.bulkProcessor = buildProcessor();
                    } catch (BulkProcessorInitExcp bulkProcessorInitExcp) {
                        log.error("", bulkProcessorInitExcp);
                        TimeUtil.sleepSec(3);
                        log.debug("buildProcessor failure sleep:{}(s)", 3);
                        continue;
                    }
                    this.connS.set(true);
                }
                break;
            }
        }
        this.bulkProcessor.add(doc);
    }


    private static class RetryFailureListener implements BulkProcessor.Listener {
        private final List<DocWriteRequest> lazyQueue;
        private final AtomicBoolean connS;

        public RetryFailureListener(List<DocWriteRequest> lazyQueue, AtomicBoolean connS) {
            this.lazyQueue = lazyQueue;
            this.connS = connS;
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
            if (failure instanceof ConnectException) {
                this.connS.set(false);
                this.lazyQueue.addAll(request.requests());
            }
        }
    }

    private synchronized BulkProcessor buildProcessor() throws BulkProcessorInitExcp {
        try {
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (rq, listener) -> this.client.bulkAsync(rq, RequestOptions.DEFAULT, listener);
            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, new RetryFailureListener(this.lazyQueue, this.connS));
            builder.setBulkActions(this.esConfig.getBatch());
            builder.setBulkSize(new ByteSizeValue(this.esConfig.getBufferWriteSize(), ByteSizeUnit.MB));
            builder.setConcurrentRequests(this.esConfig.getParallel());
//            builder.setFlushInterval(TimeValue.timeValueSeconds(this.esConfig.getBufferFlushInterval()));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(this.esConfig.getBackOffSec()), this.esConfig.getBackOffRetries()));
            return builder.build();
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
                this.addReq(request);
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
                this.addReq(request);
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
        log.info("lazyQueue.size:{}", this.lazyQueue.size());
        int i = 0;
        Iterator<DocWriteRequest> item = lazyQueue.iterator();
        while (item.hasNext() && i < 200) {
            this.addReq(this.lazyQueue.get(0));
            this.lazyQueue.remove(0);
            i++;
        }
        log.info("lazyQueue.size:{}", this.lazyQueue.size());
    }

    private void retryWriteAll() {
        if (!this.lazyQueue.isEmpty()) {
            log.warn("lazyExe,sleep:{}", this.esConfig.getReqWriteWaitMill());
            TimeUtil.sleepMill(this.esConfig.getReqWriteWaitMill());
        }
        log.info("lazyQueue.size:{}", this.lazyQueue.size());
        Iterator<DocWriteRequest> item = lazyQueue.iterator();
        while (item.hasNext()) {
            this.addReq(this.lazyQueue.get(0));
            this.lazyQueue.remove(0);
        }
        log.info("lazyQueue.size:{}", this.lazyQueue.size());
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
