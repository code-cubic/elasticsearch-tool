package com.codecubic.dao;

import com.codecubic.common.DocData;
import com.codecubic.common.FieldInfo;
import com.codecubic.common.IndexInfo;
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

    Set<String> getAllIndex();

    IndexInfo getIndexSchema(String indexName);

    @Deprecated
    boolean addNewField2Index(IndexInfo indexinf);

    boolean addNewField2Index(String index, Collection<FieldInfo> fieldInfos);

    DocData getDoc(String indexName, String docType, String id, String[] fields);

    long count(String indexName, String docType, Map<String, Object> conditions);

    boolean delByQuery(String indexName, String docType, Map<String, Object> conditions);

    List<Map<String, Object>> query(String sql);

    Set<String> getAliasByIndex(String indexName);

    Set<String> getIndexsByAlias(String indexAlias);

    boolean existIndex(String indexName);

    boolean existAlias(String indexName, String indexAlias);

    boolean updatIndxAlias(String indexName, Collection<String> newAlias, Collection<String> delAlias);

    boolean asyncBulkUpsert(String indexName, String docType, List<DocData> docs);

    boolean asyncUpsert(String indexName, String docType, DocData doc);

    boolean asyBulkDelDoc(String indexName, String docType, Collection<String> docIds);

    void upsrt(String indexName, String docType, DocData doc) throws IOException;

    RestHighLevelClient getClient();

    /**
     * where you use bulk api like asyncBulkUpsert, please use this method when appropriate !
     * if you can not use close method on your program and you use any bulk api , please make shure use this method when appropriate!
     */
    void flush();

    void close();
}
