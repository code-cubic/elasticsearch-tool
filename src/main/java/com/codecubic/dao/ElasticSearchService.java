package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.IndexInfo;

import java.util.List;

public interface ElasticSearchService {
    boolean createIndex(String indexName, String source);
    boolean createIndex(IndexInfo indexInf);
    boolean deleIndex(String indexName);
    List<String> getAllIndex();
    IndexInfo indexSchema(String indexName, String type);
    boolean addNewField2Index(IndexInfo indexinf);
    DocData getDoc(String indexName, String indexType, String id, String[] fields);
    void asyncBulkUpsert(String indexName, String indexType, List<DocData> docs);
    void close();
}
