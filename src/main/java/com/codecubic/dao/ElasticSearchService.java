package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.IndexInfo;

import java.util.List;
import java.util.Map;

public interface ElasticSearchService {
    boolean createIndex(String indexName, String source);
    boolean createIndex(IndexInfo indexInf);
    boolean deleIndex(String indexName);
    List<String> getAllIndex();
    IndexInfo indexSchema(String indexName, String type);
    boolean addNewField2Index(IndexInfo indexinf);
    DocData getDoc(String indexName, String indexType, String id, String[] fields);
    void asyncBulkUpsert(String indexName, String indexType, List<DocData> docs);
    long count(String indexName, String indexType, Map<String, Object> conditions);
    void close();
}
