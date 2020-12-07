package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.IndexInfo;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IElasticSearchService {

    boolean createIndex(String indexName, String source);

    boolean createIndex(IndexInfo indexInf);

    boolean deleIndex(String indexName);

    List<String> getAllIndex();

    IndexInfo getIndexSchema(String indexName, String docType);

    boolean addNewField2Index(IndexInfo indexinf);

    DocData getDoc(String indexName, String docType, String id, String[] fields);

    void asyncBulkUpsert(String indexName, String docType, List<DocData> docs);

    long count(String indexName, String docType, Map<String, Object> conditions);

    boolean delByQuery(String indexName, String docType, Map<String, Object> conditions);

    List<Map<String, Object>> query(String sql);

    Set<String> getAliasByIndex(String indexName);

    Set<String> getIndexsByAlias(String indexAlias);

    boolean existIndex(String indexName);

    boolean existAlias(String indexName, String indexAlias);

    boolean updatIndxAlias(String indexName, Collection<String> newAlias, Collection<String> delAlias);

    RestHighLevelClient getClient();

    void close();
}
