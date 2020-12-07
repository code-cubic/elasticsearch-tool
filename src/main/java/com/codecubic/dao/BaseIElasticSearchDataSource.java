package com.codecubic.dao;

import com.codecubic.common.*;
import com.codecubic.exception.ESInitException;
import com.codecubic.exception.NotImplemtException;
import com.codecubic.util.TimeUtil;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
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
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author code-cubic
 */
@Slf4j
public class BaseIElasticSearchDataSource implements IElasticSearchService, Closeable {

    protected ESConfig _esConf;
    protected RestHighLevelClient _client;
    /**
     * todo:使用单例processors时可能，存在意想不到的问题，需要考虑连接失败时的重新构建
     */
    protected BulkProcessor _bulkProcessor;

    public BaseIElasticSearchDataSource() {

    }

    public BaseIElasticSearchDataSource(ESConfig config) throws ESInitException {
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

    /**
     * 新增索引
     *
     * @param indexName 索引名称
     * @param source    索引定义
     */
    @Override
    public boolean createIndex(String indexName, String source) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(source, "source can not be null");

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.source(source, XContentType.JSON);
/*
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
*/
        try {
            _client.indices().create(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            log.error("create index error:", e);
        }
        return false;
    }

    @Override
    public boolean createIndex(IndexInfo indexInf) {
        Preconditions.checkNotNull(indexInf.getName(), "indexName can not be null");
        Preconditions.checkNotNull(indexInf.getType(), "docType can not be null");
        Preconditions.checkNotNull(indexInf.getPropInfo(), "propInfo can not be null");

        CreateIndexRequest request = new CreateIndexRequest(indexInf.getName());
        String indexSchemaTemplate = _esConf.getIndexSchemaTemplate();
        String source = indexSchemaTemplate.replaceAll("\\$docType", indexInf.getType())
                .replaceAll("\\$properties", indexInf.prop2JsonStr());
        request.source(source, XContentType.JSON);
        try {
            _client.indices().create(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            log.error("create index error:", e);
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName 索引名称
     * @return
     */
    @Override
    public boolean deleIndex(String indexName) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse response = _client.indices().delete(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }


    /**
     * 判断索引是否已经存在
     *
     * @param indexName 索引名称
     * @return
     */
    public boolean existIndex(String indexName) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN);
            request.indices(indexName);
            return _client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 判断指定别名是否归属指定索引
     *
     * @param indexName
     * @param indexAlias
     * @return
     */
    public boolean existIndexAlias(String indexName, String indexAlias) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(indexAlias, "indexAlias can not be null");
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName).aliases(indexAlias);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            return _client.indices().existsAlias(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 获取指定索引的所有别名
     *
     * @param indexName
     * @return
     */
    public Set<String> getIndexAlias(String indexName) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Set<String> alias = new HashSet<>(10);
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            GetAliasesResponse response = _client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = response.getAliases();
            if (aliases != null) {
                for (Map.Entry<String, Set<AliasMetaData>> entry : aliases.entrySet()) {
                    Set<AliasMetaData> value = entry.getValue();
                    if (value != null) {
                        for (AliasMetaData amd : value) {
                            alias.add(amd.getAlias());
                        }

                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return alias;
    }


    /**
     * 更新索引别名
     *
     * @param indexName 索引别名
     * @param newAlias  新增别名
     * @param delAlias  删除别名
     * @return
     */
    public boolean updatIndexAlias(String indexName, Collection<String> newAlias, Collection<String> delAlias) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        IndicesAliasesRequest req = new IndicesAliasesRequest();

        if (newAlias != null) {
            newAlias.forEach(alias -> {
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(indexName).alias(alias);
                req.addAliasAction(aliasAction);
            });
        }

        if (delAlias != null) {
            delAlias.forEach(alias -> {
                IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index(indexName).alias(alias);
                req.addAliasAction(removeAction);
            });
        }

        try {
            return _client.indices().updateAliases(req, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 获取指定别名映射的所有索引名
     *
     * @param indexAlias
     * @return
     */
    public Set<String> getIndexs(String indexAlias) {
        Preconditions.checkNotNull(indexAlias, "indexAlias can not be null");
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(indexAlias);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        try {
            Map<String, Set<AliasMetaData>> aliases = _client.indices().getAlias(request, RequestOptions.DEFAULT).getAliases();
            return aliases.keySet();
        } catch (IOException e) {
            log.error("", e);
        }
        return new HashSet<>(0);
    }

    /**
     * 获取当前集群所有索引名（索引名不是以.开头的）
     *
     * @return
     */
    @Override
    public List<String> getAllIndex() {
        List<String> list = new ArrayList<>();
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


    /**
     * 获取指定索引的定义信息
     *
     * @param indexName 索引名
     * @param type      索引type
     * @return
     */
    @Override
    public IndexInfo indexSchema(String indexName, String type) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(indexName);
        request.types(type);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        try {
            GetMappingsResponse response = _client.indices().getMapping(request, RequestOptions.DEFAULT);
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = response.mappings();
            MappingMetaData typeMapping = allMappings.get(indexName).get(type);
            Map<String, Object> mapping = typeMapping.sourceAsMap();
            if (mapping != null && !mapping.isEmpty()) {
                IndexInfo indexInf = new IndexInfo();
                indexInf.setName(indexName);
                indexInf.setType(type);
                PropertiesInfo propertiesInfo = new PropertiesInfo();
                indexInf.setPropInfo(propertiesInfo);
                LinkedHashMap properties = (LinkedHashMap) mapping.get("properties");
                properties.forEach((k, v) -> {
                    Map typeInfoMap = (Map) v;
                    FieldInfo fieldInfo = new FieldInfo();
                    fieldInfo.setName((String) k);
                    fieldInfo.setType((String) typeInfoMap.get("type"));
                    propertiesInfo.addField(fieldInfo);
                });
                return indexInf;
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return new IndexInfo();
    }


    /**
     * 为索引添加新字段
     *
     * @param indexinf
     * @return
     */
    @Override
    public boolean addNewField2Index(IndexInfo indexinf) {
        Preconditions.checkNotNull(indexinf.getName(), "indexName can not be null");
        Preconditions.checkNotNull(indexinf.getType(), "docType can not be null");
        Preconditions.checkNotNull(indexinf.getPropInfo(), "propInfo can not be null");

        PutMappingRequest request = new PutMappingRequest(indexinf.getName());
        request.type(indexinf.getType());
        request.timeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
//        request.timeout("2m");
//        request.masterNodeTimeout("1m");
        Map<String, Object> properties = new HashMap<>(indexinf.getPropInfo().getFields().size());
        for (FieldInfo fi : indexinf.getPropInfo().getFields()) {
            Map<String, Object> message = new HashMap<>();
            message.put("type", fi.getType());
            properties.put(fi.getName(), message);
        }
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("properties", properties);
        request.source(jsonMap);
        try {
            return _client.indices().putMapping(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }


    /**
     * 统计满足条件的指定索引的文档总条数
     *
     * @param indexName
     * @param docType
     * @param conditions 条件组合，每一组条件之间是and关系，每一组条件都是K=v的关系
     * @return 查询失败时，返回-1
     */
    @Override
    public long count(String indexName, String docType, Map<String, Object> conditions) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);

        CardinalityAggregationBuilder cardinality = AggregationBuilders.cardinality("count");
        cardinality.field("count");
        searchSourceBuilder.aggregation(cardinality);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(docType);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (conditions != null) {
            conditions.forEach((k, v) ->
            {
                if (v == null) {
                    boolQueryBuilder.mustNot(QueryBuilders.existsQuery(k));
                } else {
                    boolQueryBuilder.must(QueryBuilders.termQuery(k, v));
                }
            });
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            return _client.search(searchRequest, RequestOptions.DEFAULT).getHits().getTotalHits();
        } catch (IOException e) {
            log.error("", e);
        }
        return -1;
    }


    /**
     * 根据指定ID查询文档数据
     *
     * @param indexName
     * @param docType
     * @param id
     * @param fields    指定需要返回的字段名
     * @return
     */
    @Override
    public DocData getDoc(String indexName, String docType, String id, String[] fields) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        try {
            GetRequest getRequest = new GetRequest(indexName);
            getRequest.type(docType);
            getRequest.id(id);
            FetchSourceContext sourceContext;
            if (fields != null) {
                sourceContext = new FetchSourceContext(true, fields, null);
            } else {
                sourceContext = new FetchSourceContext(true, null, null);
            }
            getRequest.fetchSourceContext(sourceContext);
            GetResponse response = this._client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = response.getSource();
            DocData docData = new DocData();
            docData.setId(id);
            docData.setVersion(response.getVersion());
            source.forEach((k, v) -> {
                FieldData fieldData = new FieldData();
                fieldData.setName(k);
                fieldData.setVal(v);
                docData.addField(fieldData);
            });
            return docData;
        } catch (Exception e) {
            log.error("", e);
        }
        return new DocData();
    }

//    public List<Map<String, Object>> searchDocs(String[] indice, Map<String, Object> conditions, String[] fields, int size) {
//        List<Map<String, Object>> rel = new ArrayList<>();
//        try {
//            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//            if (conditions != null) {
//                for (String key : conditions.keySet()) {
//                    sourceBuilder.query(QueryBuilders.matchQuery(key, conditions.get(key)));
//                }
//            }
//            if (size > 0) {
//                sourceBuilder.size(size);
//            }
//            sourceBuilder.fetchSource(fields, null);
//
//            SearchRequest request = new SearchRequest(indice, sourceBuilder);
//
//            SearchResponse response = this._client.search(request, RequestOptions.DEFAULT);
//            SearchHits hits = response.getHits();
//            SearchHit[] searchHits = hits.getHits();
//            for (SearchHit hit : searchHits) {
//                // do something with the SearchHit
//                rel.add(hit.getSourceAsMap());
//            }
//            return rel;
//        } catch (IOException e) {
//            log.error("", e);
//            return null;
//        }
//    }


//    public ResultCode insertDoc(String indexName, String typeName, Long version, String id, String doc) {
//        try {
//            if (version == null || version < 0) {
//                IndexRequest indexRequest = new IndexRequest();
//                indexRequest.index(indexName);
//                indexRequest.type(typeName);
//                indexRequest.id(id);
//                indexRequest.source(doc, XContentType.JSON);
//                _client.index(indexRequest, RequestOptions.DEFAULT);
//            } else {
//                UpdateRequest updateRequest = new UpdateRequest();
//                updateRequest.index(indexName);
//                updateRequest.type(typeName);
//                updateRequest.id(id);
//                updateRequest.doc(doc, XContentType.JSON);
//                updateRequest.version(version);
//                _client.update(updateRequest, RequestOptions.DEFAULT);
//            }
//        } catch (ElasticsearchStatusException e) {
//            RestStatus status = e.status();
//            if ("CONFLICT".equals(status.name()) || 409 == status.getStatus()) {
//                String detailedMessage = e.getDetailedMessage();
//                if (detailedMessage.contains("version_conflict")) {
//                    return ResultCode.VERSION_CONFLICT;
//                }
//            }
//        } catch (IOException e) {
//            if (log.isDebugEnabled()) {
//                log.debug("insertDoc method encounter error", e);
//            }
//            return ResultCode.IOEXCEPTION;
//        }
//        return ResultCode.SUCCESS;
//    }


    private synchronized void loadProcessor() {
        if (this._bulkProcessor != null) {
            return;
        }
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
        try {
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (_rq, _listener) -> _client.bulkAsync(_rq, RequestOptions.DEFAULT, _listener);
            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, processListener);
            builder.setBulkActions(this._esConf.getBatch());
            builder.setBulkSize(new ByteSizeValue(this._esConf.getBufferWriteSize(), ByteSizeUnit.MB));
            builder.setConcurrentRequests(this._esConf.getParallel());
            builder.setFlushInterval(TimeValue.timeValueSeconds(2L));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(2L), 3));
            this._bulkProcessor = builder.build();
        } catch (Exception e) {
            log.error("", e);
            TimeUtil.sleepSec(1);
            loadProcessor();
        }
    }


    /**
     * 异步批量数据写入
     * 适合离线批处理场景，不适合实时场景
     *
     * @param indexName
     * @param docType
     * @param docs
     */
    @Override
    public void asyncBulkUpsert(String indexName, String docType, List<DocData> docs) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        Preconditions.checkNotNull(docs, "docs can not be null");
        loadProcessor();
        try {
            for (DocData doc : docs) {
                Map<String, Object> objectMap = doc.toMap();
                UpdateRequest request = new UpdateRequest(indexName, docType, doc.getId())
                        .upsert(objectMap).doc(objectMap);
                request.retryOnConflict(2);
                request.waitForActiveShards(1);
                request.timeout(TimeValue.timeValueSeconds(30));
                _bulkProcessor.add(request);
            }
        } catch (Throwable e) {
            log.error("", e);
        } finally {
            _bulkProcessor.flush();
            TimeUtil.sleepMill(_esConf.getBufferFlushWaitMill());
        }
    }

    /**
     * 异步批量数据删除，根据doc id进行删除
     * 适合离线批处理场景，不适合实时场景
     *
     * @param indexName
     * @param docType
     * @param docIds
     */
    public void asyncBulkDelDoc(String indexName, String docType, Collection<String> docIds) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        Preconditions.checkNotNull(docIds, "docIds can not be null");
        loadProcessor();
        try {
            docIds.forEach(id -> {
                DeleteRequest request = new DeleteRequest(indexName, docType, id);
                request.waitForActiveShards(1);
                _bulkProcessor.add(request);
            });
        } catch (Exception e) {
            log.error("", e);
        } finally {
            this._bulkProcessor.flush();
        }
    }


    @Override
    public boolean delByQuery(String indexName, String docType, Map<String, Object> conditions) {
        Preconditions.checkNotNull(indexName, "indexName can not be null");
        Preconditions.checkNotNull(docType, "docType can not be null");
        Preconditions.checkNotNull(conditions, "conditions can not be null");
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setDocTypes(docType);
        request.setConflicts("proceed");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        conditions.forEach((k, v) -> {
            if (v == null) {
                boolQueryBuilder.mustNot(QueryBuilders.existsQuery(k));
            } else {
                boolQueryBuilder.must(QueryBuilders.termQuery(k, v));
            }
        });
        request.setQuery(boolQueryBuilder);
        request.setBatchSize(1000);
        request.setRefresh(true);
        request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        try {
            _client.deleteByQueryAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkResponse) {

                }

                @Override
                public void onFailure(Exception e) {
                    //todo:
                }
            });
        } catch (Exception e) {
            log.error("", e);
            return false;
        }

        return true;
    }

    @Override
    public List<Map<String, Object>> query(String sql) {
        throw new NotImplemtException();
    }

    @Override
    public void close() {
        try {
            this._bulkProcessor.awaitClose(5, TimeUnit.SECONDS);
            this._client.close();
        } catch (Exception e) {
            log.error("", e);
        }
    }

}
