package com.codecubic.dao;

import com.codecubic.common.*;
import com.codecubic.exception.ESInitException;
import com.codecubic.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.yaml.snakeyaml.Yaml;

import java.util.*;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IElasticSearchServiceTest {

    private IElasticSearchService esServ;
    private IElasticSearchService esSqlServ;

    {
        Yaml yaml = new Yaml();
        ESConfig esConfig = yaml.loadAs(IElasticSearchService.class.getClassLoader().getResourceAsStream("application.yml"), ESConfig.class);
        try {
            esServ = new BaseIElasticSearchDataSource(esConfig);
            esSqlServ = new ElasticSearchSqlDataSource(esConfig);
        } catch (ESInitException e) {
            e.printStackTrace();
        }
    }

    @Order(0)
    @Test
    void existIndex() {
        Assertions.assertFalse(esServ.existIndex("index_20201101"));
    }

    @Order(1)
    @Test
    void createIndex() {
        IndexInfo indexInf = new IndexInfo();
        indexInf.setName("index_20201101");
        indexInf.setType("_doc");
        PropertiesInfo prop = new PropertiesInfo();
        indexInf.setPropInfo(prop);
        prop.addField("cid", "keyword");
        prop.addField("age", "integer");
        prop.addField("bal", "double");
        prop.addField("name", "keyword");
        Assertions.assertTrue(esServ.createIndex(indexInf));
    }

    @Order(2)
    @Test
    void getAllIndex() {
        List<String> allIndex = esServ.getAllIndex();
        Assertions.assertNotNull(allIndex);
        allIndex.forEach(index -> System.out.println(index));
    }

    @Order(2)
    @Test
    void existIndex02() {
        Assertions.assertTrue(esServ.existIndex("index_20201101"));
    }

    @Order(2)
    @Test
    void addNewField2Index() {
        IndexInfo indexInf = new IndexInfo();
        indexInf.setName("index_20201101");
        indexInf.setType("_doc");
        PropertiesInfo prop = new PropertiesInfo();
        indexInf.setPropInfo(prop);
        prop.addField("load_bal", "double");
        Assertions.assertTrue(esServ.addNewField2Index(indexInf));
    }

    @Order(3)
    @Test
    void getIndexSchema() {
        IndexInfo indexInfo = esServ.getIndexSchema("index_20201101", "_doc");
        Assertions.assertEquals("index_20201101", indexInfo.getName());
        Assertions.assertEquals("_doc", indexInfo.getType());
        ArrayList<String> names = new ArrayList<>();
        names.add("cid");
        names.add("age");
        names.add("bal");
        names.add("load_bal");
        names.add("name");
        List<FieldInfo> fields = indexInfo.getPropInfo().getFields();
        Assertions.assertNotNull(fields);
        fields.forEach(f -> {
            Assertions.assertTrue(names.contains(f.getName()));
            if (f.getName().equals("cid")) {
                Assertions.assertEquals("keyword", f.getType());
            } else if (f.getName().equals("age")) {
                Assertions.assertEquals("integer", f.getType());
            } else if (f.getName().equals("bal")) {
                Assertions.assertEquals("double", f.getType());
            } else if (f.getName().equals("load_bal")) {
                Assertions.assertEquals("double", f.getType());
            } else if (f.getName().equals("name")) {
                Assertions.assertEquals("keyword", f.getType());
            }
        });
    }

    @Order(4)
    @Test
    void asyncBulkUpsert() {
        ArrayList<DocData> docDatas = new ArrayList<>();
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 10));
        docDatas.add(doc);
        esServ.asyncBulkUpsert("index_20201101", "_doc", docDatas);
    }

    @Order(4)
    @Test
    void asyncBulkUpsert2() {
        ArrayList<DocData> docDatas = new ArrayList<>();
        for (int i = 2; i < 21; i++) {
            DocData doc = new DocData();
            String cid = "10000000" + i;
            doc.setId(cid);
            doc.addField(new FieldData("cid", cid));
            doc.addField(new FieldData("age", i));
            doc.addField(new FieldData("bal", i * 1.5));
            doc.addField(new FieldData("load_bal", 3));
            doc.addField(new FieldData("name", "姓名" + i));
            docDatas.add(doc);
        }
        esServ.asyncBulkUpsert("index_20201101", "_doc", docDatas);
    }

    @Order(5)
    @Test
    void asyncBulkUpsert3() {
        ArrayList<DocData> docDatas = new ArrayList<>();
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 20));
        doc.addField(new FieldData("bal", 20d));
        doc.addField(new FieldData("name", "姓名"));
        docDatas.add(doc);
        esServ.asyncBulkUpsert("index_20201101", "_doc", docDatas);
    }

    @Order(10)
    @Test
    void getDoc() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", new String[]{"age", "bal", "load_bal"});
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(20, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));

    }

    @Order(11)
    @Test
    void getDoc2() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", null);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(20, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));
        Assertions.assertEquals("姓名", doc.getValStr("name"));
    }

    @Order(20)
    @Test
    void count() {
        Assertions.assertEquals(20, esServ.count("index_20201101", "_doc", null));
    }

    @Order(21)
    @Test
    void count02() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertEquals(1, esServ.count("index_20201101", "_doc", paramMap));
    }

    @Order(21)
    @Test
    void count03() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertEquals(1, esServ.count("index_20201101", "_doc", paramMap));
    }

    @Order(21)
    @Test
    void count04() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        Assertions.assertEquals(1, esServ.count("index_20201101", "_doc", paramMap));
    }

    @Order(25)
    @Test
    void query() {
        TimeUtil.sleepSec(2);
        String sql = "select age,count(1) as ct from index_20201101 where age > 19 group by age";
        List<Map<String, Object>> list = esSqlServ.query(sql);
        Assertions.assertNotNull(list);
        Assertions.assertTrue(list.size() > 0);
        list.forEach(map -> {
            Assertions.assertEquals(2, map.size());
            Assertions.assertEquals(2, map.get("ct"));
            Assertions.assertEquals(20, map.get("age"));
        });
    }

    @Order(30)
    @Test
    void delByQuery() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertTrue(esServ.delByQuery("index_20201101", "_doc", paramMap));
        TimeUtil.sleepSec(2);
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", null);
        Assertions.assertNull(doc.getId());
    }

    @Order(35)
    @Test
    void updatIndxAlias() {
        ArrayList<String> alias = new ArrayList<>();
        alias.add("index_a");
        alias.add("index_b");
        Assertions.assertTrue(esServ.updatIndxAlias("index_20201101", alias, null));
    }

    @Order(36)
    @Test
    void getAliasByIndex() {
        Set<String> alias = esServ.getAliasByIndex("index_20201101");
        alias.forEach(a -> Assertions.assertTrue(a.equals("index_a") || a.equals("index_b")));
    }

    @Order(37)
    @Test
    void updatIndxAlias02() {
        ArrayList<String> alias = new ArrayList<>();
        alias.add("index_a");
        Assertions.assertTrue(esServ.updatIndxAlias("index_20201101", null, alias));
        esServ.getAliasByIndex("index_20201101").forEach(a -> Assertions.assertTrue(a.equals("index_b")));
    }

    @Order(38)
    @Test
    void getIndexsByAlias() {
        Set<String> indexs = esServ.getIndexsByAlias("index_b");
        Assertions.assertNotNull(indexs);
        Assertions.assertFalse(indexs.isEmpty());
        indexs.forEach(e -> Assertions.assertEquals("index_20201101", e));
    }

    @Order(38)
    @Test
    void existAlias() {
        Assertions.assertTrue(esServ.existAlias("index_20201101", "index_b"));
    }

    @Order(998)
    @Test
    void deleIndex() {
        Assertions.assertTrue(esServ.deleIndex("index_20201101"));
    }

    @Order(999)
    @Test
    void close() {
        esServ.close();
    }


}