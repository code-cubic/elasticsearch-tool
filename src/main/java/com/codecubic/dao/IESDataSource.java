package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.IndexInfo;
import com.codecubic.exception.BulkProcessorInitExcp;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IESDataSource {

    boolean createIndex(String indexName, String source);

    boolean createIndex(IndexInfo indexInf);

    boolean deleIndex(String indexName);

    List<String> getAllIndex();

    IndexInfo getIndexSchema(String indexName, String docType);

    boolean addNewField2Index(IndexInfo indexinf);

    DocData getDoc(String indexName, String docType, String id, String[] fields);

    boolean asyncBulkUpsert(String indexName, String docType, List<DocData> docs) throws BulkProcessorInitExcp;

    long count(String indexName, String docType, Map<String, Object> conditions);

    boolean delByQuery(String indexName, String docType, Map<String, Object> conditions) throws BulkProcessorInitExcp;

    List<Map<String, Object>> query(String sql);

    Set<String> getAliasByIndex(String indexName);

    Set<String> getIndexsByAlias(String indexAlias);

    boolean existIndex(String indexName);

    boolean existAlias(String indexName, String indexAlias);

    boolean updatIndxAlias(String indexName, Collection<String> newAlias, Collection<String> delAlias);

    boolean asyBulkDelDoc(String indexName, String docType, Collection<String> docIds) throws BulkProcessorInitExcp;

    void upsrt(String indexName, String docType, DocData doc) throws IOException;

    RestHighLevelClient getClient();

    void flush();

    void close();
}
