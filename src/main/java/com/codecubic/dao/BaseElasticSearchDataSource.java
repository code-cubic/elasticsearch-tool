package com.codecubic.dao;

import com.codecubic.common.*;
import com.codecubic.exception.ESInitException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
/**
 * @author code-cubic
 */
@Slf4j
public class BaseElasticSearchDataSource {

    protected ESConfig _esConf;
    protected RestHighLevelClient _client;
    /**
     * todo:使用单例processors时可能，存在意想不到的问题
     */
    protected BulkProcessor processor;

    public BaseElasticSearchDataSource(ESConfig config) throws ESInitException {
        this._esConf = config;

        try {
            String[] split = StringUtils.split(this._esConf.getHttpHostInfo(), ",");
            List<HttpHost> httpHosts = Arrays.stream(split).map(e -> {
                String[] host = StringUtils.split(e, ":");
                return new HttpHost(host[0], Integer.parseInt(host[1]));
            }).collect(Collectors.toList());
            HttpHost[] httpHosts1 = new HttpHost[httpHosts.size()];
            httpHosts.toArray(httpHosts1);
            RestClientBuilder clientBuilder = RestClient.builder(httpHosts1);

            clientBuilder.setRequestConfigCallback(builder ->
                    builder.setConnectTimeout(this._esConf.getConnectTimeoutMillis())
                            .setSocketTimeout(this._esConf.getSocketTimeoutMillis())
                            .setConnectionRequestTimeout(this._esConf.getConnectionRequestTimeoutMillis()));
            //设置节点选择器
            clientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            clientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder.setDefaultIOReactorConfig(
                                    IOReactorConfig.custom()
                                            .setIoThreadCount(this._esConf.getIoThreadCount()).build())
                                    .setMaxConnPerRoute(this._esConf.getMaxConnectPerRoute())
                                    .setMaxConnTotal(this._esConf.getMaxConnectTotal())
            );
            //设置监听器，每次节点失败都可以监听到，可以作额外处理
            clientBuilder.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    super.onFailure(node);
                    log.error("node:{} connect failure！", node.getHost());
                }
            }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
            this._client = new RestHighLevelClient(clientBuilder);
        } catch (Exception e) {
            log.error("", e);
            throw new ESInitException(e);
        }
    }


    public Tuple<Long, Map<String, Object>> getDoc(String indexName, String id, String[] fields) {
        return getDoc(indexName, null, id, fields);
    }

    public Tuple<Long, Map<String, Object>> getDoc(String indexName, String typeName, String id, String[] fields) {
        try {
            GetRequest getRequest = new GetRequest(indexName);
            if (typeName != null && !typeName.isEmpty()) {
                getRequest.type(typeName);
            }
            getRequest.id(id);

            FetchSourceContext sourceContext = new FetchSourceContext(true, fields, null);
            getRequest.fetchSourceContext(sourceContext);
            GetResponse response = this._client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = response.getSource();
            Tuple<Long, Map<String, Object>> versionDataMap = new Tuple<>(response.getVersion(), source);
            return versionDataMap;
        } catch (IOException e) {
            log.error("", e);
            return null;
        }
    }


    public List<Map<String, Object>> searchDocs(String[] indice, Map<String, Object> conditions, String[] fields, int size) {
        RestHighLevelClient client;
        List<Map<String, Object>> rel = new ArrayList<>();
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            if (conditions != null) {
                for (String key : conditions.keySet()) {
                    sourceBuilder.query(QueryBuilders.matchQuery(key, conditions.get(key)));
                }
            }
            if (size > 0) {
                sourceBuilder.size(size);
            }
            sourceBuilder.fetchSource(fields, null);

            SearchRequest request = new SearchRequest(indice, sourceBuilder);

            SearchResponse response = this._client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit
                rel.add(hit.getSourceAsMap());
            }
            return rel;
        } catch (IOException e) {
            log.error("", e);
            return null;
        }
    }

    public ResultCode insertDoc(String indexName, Long version, String id, String doc) {
        return insertDoc(indexName, "_doc", version, id, doc);
    }

    public ResultCode insertDoc(String indexName, String typeName, Long version, String id, String doc) {
        try {
            if (version == null || version < 0) {
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.index(indexName);
                indexRequest.type(typeName);
                indexRequest.id(id);
                indexRequest.source(doc, XContentType.JSON);
                _client.index(indexRequest, RequestOptions.DEFAULT);
            } else {
                UpdateRequest updateRequest = new UpdateRequest();
                updateRequest.index(indexName);
                updateRequest.type(typeName);
                updateRequest.id(id);
                updateRequest.doc(doc, XContentType.JSON);
                updateRequest.version(version);
                _client.update(updateRequest, RequestOptions.DEFAULT);
            }
        } catch (ElasticsearchStatusException e) {
            RestStatus status = e.status();
            if ("CONFLICT".equals(status.name()) || 409 == status.getStatus()) {
                String detailedMessage = e.getDetailedMessage();
                if (detailedMessage.contains("version_conflict")) {
                    return ResultCode.VERSION_CONFLICT;
                }
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("insertDoc method encounter error", e);
            }
            return ResultCode.IOEXCEPTION;
        }
        return ResultCode.SUCCESS;
    }

    public boolean existIndex(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN);
            request.indices(indexName);
            boolean exists = _client.indices().exists(request, RequestOptions.DEFAULT);
            return exists;
        } catch (IOException e) {
            return existIndex(indexName);
        }
    }

    public boolean existIndexAlias(String indexName, String alias) {
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName).aliases(alias);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            boolean exists = _client.indices().existsAlias(request, RequestOptions.DEFAULT);
            return exists;

        } catch (IOException e) {
            return existIndexAlias(indexName, alias);
        }
    }

    public Set<String> getIndex(String alias) {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(alias);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        try {
            Map<String, Set<AliasMetaData>> aliases = _client.indices().getAlias(request, RequestOptions.DEFAULT).getAliases();
            return aliases.keySet();
        } catch (IOException e) {
            log.error("", e);
            return new HashSet<>(0);
        }
    }

    /**
     * 删除老的别名，增加新的别名
     *
     * @param aliasOperMap k:操作类型，v: first col -> indexName ;second col -> alias
     */
    public void updateAlias(Map<String, List<Tuple<String, String>>> aliasOperMap) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        for (Map.Entry<String, List<Tuple<String, String>>> entry : aliasOperMap.entrySet()) {
            String type = entry.getKey();
            List<Tuple<String, String>> list = entry.getValue();
            for (Tuple<String, String> tup : list) {
                if ("delete".equals(type)) {
                    if (existIndexAlias(tup.getV1(), tup.getV2())) {
                        IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index(tup.getV1()).alias(tup.getV2());
                        request.addAliasAction(removeAction);
                    }
                } else if ("add".equals(type)) {
                    if (existIndex(tup.getV1())) {
                        IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(tup.getV1()).alias(tup.getV2());
                        request.addAliasAction(aliasAction);
                    }
                }
            }
        }
        try {
            _client.indices().updateAliases(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public long count(String[] indice, String type, Map<String, Object> conditions) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);

        CardinalityAggregationBuilder cardinality = AggregationBuilders.cardinality("count");
        cardinality.field("count");
        searchSourceBuilder.aggregation(cardinality);

        SearchRequest searchRequest = new SearchRequest(indice);
        if (conditions != null) {
            for (String key : conditions.keySet()) {
                searchSourceBuilder.query(QueryBuilders.matchQuery(key, conditions.get(key)));
            }
        }
        if (type != null) {
            searchRequest.types(type);
        }

        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse sr = _client.search(searchRequest, RequestOptions.DEFAULT);
            long totalHits = sr.getHits().getTotalHits();
            return totalHits;
        } catch (IOException e) {
            log.error("", e);
        }
        return -1;
    }

    public void createIndex(String indexName, String source) {
        if (indexName == null) {
            log.error("indexName is null,please check!");
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.source(source, XContentType.JSON);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        try {
            _client.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("create index error:", e);
        }
    }

    private BulkProcessor.Builder createBuilder() {
        BulkProcessor.Listener processListener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("start batching,executionId:{}", executionId);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                for (BulkItemResponse bulkItemResponse : response) {
                    if (bulkItemResponse.isFailed()) {
                        log.error(response.buildFailureMessage());
                        log.error("batch partial failure,executionId:{}", executionId);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("", failure);
            }
        };
        if (_client == null) {
            log.error("client is null!!!!!!!!!!!!!!!");
        }
        try {
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (_rq, _listener) -> _client.bulkAsync(_rq, RequestOptions.DEFAULT, _listener);
            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, processListener);
            builder.setBulkActions(this._esConf.getBatch());
            builder.setBulkSize(new ByteSizeValue(this._esConf.getBufferWriteSize(), ByteSizeUnit.MB));
            builder.setConcurrentRequests(this._esConf.getParallel());
            builder.setFlushInterval(TimeValue.timeValueSeconds(2L));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(2L), 3));
            return builder;
        } catch (Exception e) {
            log.error("", e);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
        return createBuilder();
    }

    public void updateIndex(UpdateSettingsRequest updateReq) {
        try {
            _client.indices().putSettings(updateReq, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public boolean deleIndex(String index) {
        if (index != null) {
            try {
                DeleteIndexRequest request = new DeleteIndexRequest(index);
                AcknowledgedResponse response = _client.indices().delete(request, RequestOptions.DEFAULT);
                return response.isAcknowledged();
            } catch (Exception e) {
                log.error("", e);
            }
        }
        return false;
    }

    public List<String> getAliasByIndex(String indexs) {
        ArrayList<String> aliasList = new ArrayList<>();
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexs);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            GetAliasesResponse response = _client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = response.getAliases();
            if (aliases != null) {
                for (Map.Entry<String, Set<AliasMetaData>> entry : aliases.entrySet()) {
                    Set<AliasMetaData> value = entry.getValue();
                    if (value != null) {
                        for (AliasMetaData amd : value) {
                            aliasList.add(amd.getAlias());
                        }

                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return aliasList;
    }


    public List<String> listIndex() {
        ArrayList<String> list = new ArrayList<>();
        try {
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = _client.cluster().health(request, RequestOptions.DEFAULT);
            Map<String, ClusterIndexHealth> indices = response.getIndices();
            if (indices != null) {
                for (String index : indices.keySet()) {
                    if (!index.startsWith(".")) {
                        list.add(index);
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return list;
    }

    public Optional<IndexInfo> getIndexMapping(String indexName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(indexName);
        request.types("_doc");
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        try {
            GetMappingsResponse response = _client.indices().getMapping(request, RequestOptions.DEFAULT);
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = response.mappings();
            MappingMetaData typeMapping = allMappings.get(indexName).get("_doc");
            Map<String, Object> mapping = typeMapping.sourceAsMap();
            if (mapping != null && !mapping.isEmpty()) {
                IndexInfo indexInfo = new IndexInfo();
                indexInfo.setName(indexName);
                indexInfo.setType("_doc");
                PropertiesInfo propertiesInfo = new PropertiesInfo();
                indexInfo.setPropInfo(propertiesInfo);
                LinkedHashMap properties = (LinkedHashMap) mapping.get("properties");
                properties.forEach((k, v) -> {
                    Map typeInfoMap = (Map) v;
                    FieldInfo fieldInfo = new FieldInfo();
                    fieldInfo.setName((String) k);
                    fieldInfo.setType((String) typeInfoMap.get("type"));
                    propertiesInfo.addField(fieldInfo);
                });
                return Optional.ofNullable(indexInfo);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return Optional.empty();
    }

    public boolean addNewField2Index(String indexName, List<FieldInfo> fieldInfos) {
        PutMappingRequest request = new PutMappingRequest(indexName);
        request.type("_doc");
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout("1m");
        Map<String, Object> properties = new HashMap<>();
        for (FieldInfo fi : fieldInfos) {
            Map<String, Object> message = new HashMap<>();
            message.put("type", fi.getType());
            properties.put(fi.getName(), message);
        }
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("properties", properties);
        request.source(jsonMap);
        try {
            AcknowledgedResponse response = _client.indices().putMapping(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    private synchronized BulkProcessor loadProcessor(boolean reload) {
        if (processor == null || reload) {
            BulkProcessor build = createBuilder().build();
            if (build != null) {
                processor = build;
            }
        }
        return processor;
    }

    public void bulkUpsertAsync(boolean useSingle, Integer waitSec, String indexName, List<DocInfo> documentInfos, boolean erroExist) {
        if (indexName == null || documentInfos == null) {
            log.error("indexName or documentInfos is null,please check!");
            return;
        }
        int errCount = 0;
        while (errCount < 3) {
            try {
                if (useSingle) {
                    processor = loadProcessor(false);
                } else {
                    processor = getProcess();
                }
                for (DocInfo doc : documentInfos) {
                    Map<String, Object> jsonMap = new HashMap<>();
                    for (FieldInfo f : doc.getFieldInfoList()) {
                        jsonMap.put(f.getName(), f.getVal());
                    }
                    UpdateRequest request = new UpdateRequest(indexName, "_doc", doc.getId())
                            .upsert(jsonMap).doc(jsonMap);
                    request.retryOnConflict(1);
                    request.waitForActiveShards(1);
                    request.timeout(TimeValue.timeValueSeconds(30));
                    processor.add(request);
                }
                if (!useSingle) {
                    processor.awaitClose(waitSec == null ? 15 : waitSec, TimeUnit.SECONDS);
                }
                break;
            } catch (Throwable e) {
                errCount++;
                if (processor != null) {
                    processor.close();
                }
                log.error("", e);
            }
        }
        if (errCount == 3 && erroExist) {
            System.exit(-1);
        }
    }

    public void bulkDelByIdAsync(boolean useSingle, Integer waitSec, String indexName, List<DocInfo> documentInfos) {
        if (indexName == null || documentInfos == null) {
            log.error("indexName or documentInfos is null,please check!");
            return;
        }
        try {
            if (useSingle) {
                processor = loadProcessor(false);
            } else {
                processor = getProcess();
            }
            for (DocInfo doc : documentInfos) {
                DeleteRequest request = new DeleteRequest(indexName, "_doc", doc.getId());
                request.waitForActiveShards(1);
                processor.add(request);
            }
            if (!useSingle) {
                processor.awaitClose(waitSec == null ? 15 : waitSec, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            if (processor != null) {
                processor.close();
            }
            log.error("", e);
            this.bulkDelByIdAsync(useSingle, waitSec, indexName, documentInfos);
        }
    }

    public void aWaitCloseProcessor(Integer waitSec) {
        try {
            if (processor != null) {
                processor.awaitClose(waitSec == null ? 15 : waitSec, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }


    private BulkProcessor getProcess() {
        return createBuilder().build();
    }

    public void bulkWriteAsync(String indexName, String type, List<Tuple<String, String>> datas, Long awaitSecond) {
        if (indexName == null) {
            log.error("indexName is null,please check!");
            return;
        }
        awaitSecond = awaitSecond == null ? 15 : awaitSecond;
        BulkProcessor processor = null;
        try {
            processor = getProcess();
            for (int i = 0; i < datas.size(); i++) {
                Tuple<String, String> tuple = datas.get(i);
                IndexRequest doc;
                if (tuple.getV1() == null) {
                    doc = new IndexRequest(indexName, type);
                } else {
                    doc = new IndexRequest(indexName, type, tuple.getV1());
                }
                doc.waitForActiveShards(1);
                try {
                    doc.source(tuple.getV2().getBytes("UTF-8"), XContentType.JSON);
                    processor.add(doc);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            processor.flush();
            processor.awaitClose(awaitSecond, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            if (processor != null) {
                processor.close();
            }
            e.printStackTrace();
            bulkWriteAsync(indexName, type, datas, awaitSecond);
        }

    }

}
