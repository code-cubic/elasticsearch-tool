package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.IndexInfo;
import com.codecubic.exception.NotImplemtException;

import java.util.List;
import java.util.Map;

public interface IElasticSearchService {

    boolean createIndex(String indexName, String source);

    boolean createIndex(IndexInfo indexInf);

    boolean deleIndex(String indexName);

    List<String> getAllIndex();

    IndexInfo indexSchema(String indexName, String docType);

    boolean addNewField2Index(IndexInfo indexinf);

    DocData getDoc(String indexName, String docType, String id, String[] fields);

    void asyncBulkUpsert(String indexName, String docType, List<DocData> docs);

    long count(String indexName, String docType, Map<String, Object> conditions);

    boolean delByQuery(String indexName, String docType, Map<String, Object> conditions);

    List<Map<String, Object>> query(String sql);

    void close();
}
