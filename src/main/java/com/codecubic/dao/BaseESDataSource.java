package com.codecubic.dao;

import com.codecubic.common.*;
import com.codecubic.exception.BulkPrcesrIntExcep;
import com.codecubic.exception.ESCliInitExcep;
import com.codecubic.exception.NotImplExcep;
import com.codecubic.util.TimeUtil;
import com.codecubic.util.Utils;
import com.google.common.base.Preconditions;
import lombok.Getter;
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
import java.util.stream.Collectors;

/**
 * @author code-cubic
 */
@Slf4j
public class BaseESDataSource implements IESDataSource, Closeable {

    protected ESConfig esConf;
    @Getter
    protected RestHighLevelClient client;
    protected RetryBulkProcessor retryBulkProcessor;


    public BaseESDataSource(ESConfig config) throws ESCliInitExcep {
        this.esConf = config;
        initClient();
    }

    protected synchronized void initClient() throws ESCliInitExcep {

        Utils.close(this::getClient);

        try {
            String[] split = StringUtils.split(this.esConf.getHttpHostInfo(), ",");
            List<HttpHost> httpHosts = Arrays.stream(split).map(e -> {
                String[] host = StringUtils.split(e, ":");
                return new HttpHost(host[0], Integer.parseInt(host[1]));
            }).collect(Collectors.toList());
            HttpHost[] subHosts = new HttpHost[httpHosts.size()];
            httpHosts.toArray(subHosts);
            RestClientBuilder clientBuilder = RestClient.builder(subHosts);

            clientBuilder.setRequestConfigCallback(builder ->
                    builder.setConnectTimeout(this.esConf.getConnectTimeoutMillis())
                            .setSocketTimeout(this.esConf.getSocketTimeoutMillis())
                            .setConnectionRequestTimeout(this.esConf.getConnectionRequestTimeoutMillis()));
            clientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            clientBuilder.setHttpClientConfigCallback(
                    httpAsyncClientBuilder ->
                            httpAsyncClientBuilder.setDefaultIOReactorConfig(
                                    IOReactorConfig.custom()
                                            .setIoThreadCount(this.esConf.getIoThreadCount()).build())
                                    .setMaxConnPerRoute(this.esConf.getMaxConnectPerRoute())
                                    .setMaxConnTotal(this.esConf.getMaxConnectTotal())
            );
            clientBuilder.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    super.onFailure(node);
                    log.error("node:{} connect failure！", node.getHost());
                }
            }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
            this.client = new RestHighLevelClient(clientBuilder);
            log.info("init client ok!");
        } catch (Exception e) {
            log.error("", e);
            throw new ESCliInitExcep(e);
        }
    }


    /**
     * @param indexName
     * @param source    es index json mapping
     */
    @Override
    public boolean createIndex(String indexName, String source) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            Preconditions.checkNotNull(source, "source can not be null");

            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.source(source, XContentType.JSON);
            this.client.indices().create(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            log.error("create index error:", e);
        }
        return false;
    }

    @Override
    public boolean createIndex(IndexInfo indexInf) {
        try {
            Preconditions.checkNotNull(indexInf.getName(), "indexName can not be null");
            Preconditions.checkNotNull(indexInf.getType(), "docType can not be null");
            Preconditions.checkNotNull(indexInf.getPropInfo(), "propInfo can not be null");

            CreateIndexRequest request = new CreateIndexRequest(indexInf.getName());
            String indexSchemaTemplate = this.esConf.getIndexSchemaTemplate();
            String source = indexSchemaTemplate.replaceAll("\\$docType", indexInf.getType())
                    .replaceAll("\\$properties", indexInf.prop2JsonStr());
            request.source(source, XContentType.JSON);
            this.client.indices().create(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            log.error("create index error:", e);
        }
        return false;
    }

    /**
     * @param indexName
     * @return
     */
    @Override
    public boolean deleIndex(String indexName) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse response = this.client.indices().delete(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }


    /**
     * @param indexName
     * @return true: exist , false : not exist
     */
    @Override
    public boolean existIndex(String indexName) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            GetIndexRequest request = new GetIndexRequest();
            request.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN);
            request.indices(indexName);
            return this.client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * @param indexName
     * @param indexAlias
     * @return
     */
    @Override
    public boolean existAlias(String indexName, String indexAlias) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            Preconditions.checkNotNull(indexAlias, "indexAlias can not be null");
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName).aliases(indexAlias);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            return this.client.indices().existsAlias(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * get index all alias
     *
     * @param indexName
     * @return
     */
    @Override
    public Set<String> getAliasByIndex(String indexName) {
        Set<String> alias = new HashSet<>();
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            GetAliasesResponse response = this.client.indices().getAlias(request, RequestOptions.DEFAULT);
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
     * @param indexName
     * @param newAlias
     * @param delAlias
     * @return
     */
    @Override
    public boolean updatIndxAlias(String indexName, Collection<String> newAlias, Collection<String> delAlias) {

        try {
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
            return this.client.indices().updateAliases(req, RequestOptions.DEFAULT).isAcknowledged();
        } catch (IOException e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * @param indexAlias
     * @return
     */
    @Override
    public Set<String> getIndexsByAlias(String indexAlias) {
        try {
            Preconditions.checkNotNull(indexAlias, "indexAlias can not be null");
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexAlias);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            Map<String, Set<AliasMetaData>> aliases = this.client.indices().getAlias(request, RequestOptions.DEFAULT).getAliases();
            return aliases.keySet();
        } catch (IOException e) {
            log.error("", e);
        }
        return new HashSet<>(0);
    }

    /**
     * get all index name on cluster and not return index which name start with .
     *
     * @return
     */
    @Override
    public Set<String> getAllIndex() {
        Set<String> list = new HashSet<>();
        try {
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = this.client.cluster().health(request, RequestOptions.DEFAULT);
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
     * @param indexName
     * @param type
     * @return
     */
    @Override
    public IndexInfo getIndexSchema(String indexName, String type) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices(indexName);
            request.types(type);
            request.indicesOptions(IndicesOptions.lenientExpandOpen());
            GetMappingsResponse response = this.client.indices().getMapping(request, RequestOptions.DEFAULT);
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = response.mappings();
            ImmutableOpenMap<String, MappingMetaData> indexMaper = allMappings.get(indexName);
            if (indexMaper != null) {
                MappingMetaData typeMapping = indexMaper.get(type);

                if (typeMapping != null) {
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
                            try {
                                if ("nested".equalsIgnoreCase(fieldInfo.getType()) || "object".equalsIgnoreCase(fieldInfo.getType())) {
                                    Map subProps = (Map) typeInfoMap.get("properties");
                                    subProps.forEach((subK, subV) -> {
                                        FieldInfo subField = new FieldInfo();
                                        Map subMap = (Map) subV;
                                        subField.setName((String) subK);
                                        subField.setType((String) subMap.get("type"));
                                        fieldInfo.addFields(subField);
                                    });
                                }
                            } catch (Exception e) {
                                log.error("", e);
                            }
                            propertiesInfo.addField(fieldInfo);
                        });
                        return indexInf;
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return new IndexInfo();
    }


    /**
     * @param indexinf
     * @return
     */
    @Override
    public boolean addNewField2Index(IndexInfo indexinf) {
        return this.addNewField2Index(indexinf.getName(), indexinf.getType(), indexinf.getPropInfo().getFields());
    }

    @Override
    public boolean addNewField2Index(String index, String type, Collection<FieldInfo> fieldInfos) {
        try {
            Preconditions.checkNotNull(index, "indexName can not be null");
            Preconditions.checkNotNull(type, "docType can not be null");
            Preconditions.checkNotNull(fieldInfos, "propInfo can not be null");

            PutMappingRequest request = new PutMappingRequest(index);
            request.type(type);
            request.timeout(TimeValue.timeValueMinutes(1));
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            Map<String, Object> properties = new HashMap<>(fieldInfos.size());
            for (FieldInfo fi : fieldInfos) {
                Map<String, Object> message = new HashMap<>();
                if ("nested".equalsIgnoreCase(fi.getType()) || "object".equalsIgnoreCase(fi.getType())) {
                    message.put("properties", fi.getInnerFieldTypeMap());
                }
                message.put("type", fi.getType());
                properties.put(fi.getName(), message);
            }
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("properties", properties);
            request.source(jsonMap);
            return this.client.indices().putMapping(request, RequestOptions.DEFAULT).isAcknowledged();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }


    /**
     * count data number when meet conditions
     *
     * @param indexName
     * @param docType
     * @param conditions
     * @return -1: when exception occurence
     */
    @Override
    public long count(String indexName, String docType, Map<String, Object> conditions) {
        try {
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
            return this.client.search(searchRequest, RequestOptions.DEFAULT).getHits().getTotalHits();
        } catch (IOException e) {
            log.error("", e);
        }
        return -1;
    }


    /**
     * @param indexName
     * @param docType
     * @param id
     * @param returnFields
     * @return
     */
    @Override
    public DocData getDoc(String indexName, String docType, String id, String[] returnFields) {
        try {
            Preconditions.checkNotNull(indexName, "indexName can not be null");
            Preconditions.checkNotNull(docType, "docType can not be null");
            GetRequest getRequest = new GetRequest(indexName);
            getRequest.type(docType);
            getRequest.id(id);
            FetchSourceContext sourceContext;
            if (returnFields != null) {
                sourceContext = new FetchSourceContext(true, returnFields, null);
            } else {
                sourceContext = new FetchSourceContext(true, null, null);
            }
            getRequest.fetchSourceContext(sourceContext);
            GetResponse response = this.client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = response.getSource();
            DocData docData = new DocData();
            if (source == null) {
                return docData;
            }
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

    @Override
    public void upsrt(String indexName, String docType, DocData doc) throws IOException {
        Map<String, Object> objectMap = doc.toMap();
        UpdateRequest request = new UpdateRequest(indexName, docType, doc.getId())
                .upsert(objectMap).doc(objectMap);
        request.retryOnConflict(10);
        request.waitForActiveShards(1);
        request.timeout(TimeValue.timeValueSeconds(this.esConf.getReqWriteWaitMill()));
        this.client.update(request, RequestOptions.DEFAULT);
    }

    @Override
    public void flush() {
        if (this.retryBulkProcessor != null) {
            this.retryBulkProcessor.flush();
        }
    }

    private synchronized void loadBulkProcessor() {
        while (this.retryBulkProcessor == null) {
            try {
                this.retryBulkProcessor = new RetryBulkProcessor(this.client, this.esConf);
                return;
            } catch (BulkPrcesrIntExcep bulkPrcesrIntExcep) {
                log.error("", bulkPrcesrIntExcep);
            }
            TimeUtil.sleepMill(5000);
        }
    }

    @Override
    public boolean asyncBulkUpsert(String indexName, String docType, List<DocData> docs) {
        loadBulkProcessor();
        return this.retryBulkProcessor.asyncBulkUpsert(indexName, docType, docs);
    }

    @Override
    public boolean asyncUpsert(String indexName, String docType, DocData doc) {
        loadBulkProcessor();
        return this.retryBulkProcessor.asyncUpsert(indexName, docType, doc);
    }


    /**
     * @param indexName
     * @param docType
     * @param docIds
     * @return true:submit suss
     */
    public boolean asyBulkDelDoc(String indexName, String docType, Collection<String> docIds) {
        loadBulkProcessor();
        return this.retryBulkProcessor.asyBulkDelDoc(indexName, docType, docIds);
    }

    /**
     * @param indexName
     * @param docType
     * @param conditions
     * @return true:submit suss
     */
    public boolean delByQuery(String indexName, String docType, Map<String, Object> conditions) {
        try {
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
            this.client.deleteByQueryAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("", e);
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
        throw new NotImplExcep();
    }


    @Override
    public void close() {
        Utils.close(this.retryBulkProcessor);
        Utils.close(this.client);
    }

}
