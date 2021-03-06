package com.codecubic.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.*;
import com.codecubic.exception.ESCliInitExcep;
import com.codecubic.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IESDataSourceTest {

    private static IESDataSource esServ;
    private static IESDataSource esSqlServ;
    private static final int WAIT_FLUSH_SEC = 2;
    private static final String INDEX_NAME = "index_20201101";

    static {
        Yaml yaml = new Yaml();
        ESConfig esConfig = yaml.loadAs(IESDataSource.class.getClassLoader().getResourceAsStream("application.yml"), ESConfig.class);
        try {
            esServ = new BaseESDataSource(esConfig);
            esSqlServ = new ESDataSource(esConfig);
        } catch (ESCliInitExcep e) {
            e.printStackTrace();
        }
    }

    @Order(0)
    @Test
    void existIndex() {
        esServ.deleIndex(INDEX_NAME);
        Assertions.assertFalse(esServ.existIndex(INDEX_NAME));
    }

    @Order(1)
    @Test
    void createIndex() {
        IndexInfo indexInf = new IndexInfo();
        indexInf.setName(INDEX_NAME);
        indexInf.setType("_doc");
        PropertiesInfo prop = new PropertiesInfo();
        indexInf.setPropInfo(prop);
        prop.addField("cid", "keyword");
        prop.addField("age", "integer");
        prop.addField("bal", "double");
        prop.addField("name", "keyword");
        FieldInfo nestedTest = new FieldInfo("nested_test", "nested");
        nestedTest.addFields(new FieldInfo("prd", "keyword"));
        nestedTest.addFields(new FieldInfo("bal", "double"));
        prop.addFields(nestedTest);
        Assertions.assertTrue(esServ.createIndex(indexInf));
    }

    @Order(2)
    @Test
    void getAllIndex() {
        Set<String> allIndex = esServ.getAllIndex();
        Assertions.assertNotNull(allIndex);
        allIndex.forEach(index -> System.out.println(index));
    }

    @Order(2)
    @Test
    void existIndex02() {
        Assertions.assertTrue(esServ.existIndex(INDEX_NAME));
    }

    @Order(2)
    @Test
    void addNewField2Index() {
        IndexInfo indexInf = new IndexInfo();
        indexInf.setName(INDEX_NAME);
        indexInf.setType("_doc");
        PropertiesInfo prop = new PropertiesInfo();
        indexInf.setPropInfo(prop);
        prop.addField("load_bal", "double");
        FieldInfo nestedTest = new FieldInfo("nested_test2", "nested");
        nestedTest.addFields(new FieldInfo("prd", "keyword"));
        nestedTest.addFields(new FieldInfo("bal", "double"));
        prop.addFields(nestedTest);
        Assertions.assertTrue(esServ.addNewField2Index(indexInf));
    }

    @Order(3)
    @Test
    void getIndexSchema() {
        IndexInfo indexInfo = esServ.getIndexSchema(INDEX_NAME, "_doc");
        Assertions.assertEquals(INDEX_NAME, indexInfo.getName());
        Assertions.assertEquals("_doc", indexInfo.getType());
        ArrayList<String> names = new ArrayList<>();
        names.add("cid");
        names.add("age");
        names.add("bal");
        names.add("load_bal");
        names.add("name");
        names.add("nested_test");
        names.add("nested_test2");
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

    @Order(3)
    @Test
    void getIndexSchema02() {
        IndexInfo indexInfo = esServ.getIndexSchema(INDEX_NAME, "_doc");
        Assertions.assertEquals(INDEX_NAME, indexInfo.getName());
        Assertions.assertEquals("_doc", indexInfo.getType());
    }

    @Order(4)
    @Test
    void asyncUpsert() throws IOException {
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 10));
        esServ.upsrt(INDEX_NAME, "_doc", doc);
        esServ.flush();
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
            doc.addField(new FieldData("load_bal", 3d));
            doc.addField(new FieldData("name", "姓名" + i));
            JSONArray nestedTestVals = new JSONArray();
            JSONObject val1 = new JSONObject();
            val1.put("prd", "hhh");
            val1.put("bal", 2000);
            nestedTestVals.add(val1);
            JSONObject val2 = new JSONObject();
            val2.put("prd", "hhh2");
            val2.put("bal", 50.55);
            nestedTestVals.add(val2);
            doc.addField(new FieldData("nested_test", nestedTestVals));
            docDatas.add(doc);
        }
        esServ.asyncBulkUpsert(INDEX_NAME, "_doc", docDatas);
        esServ.flush();
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
        esServ.asyncBulkUpsert(INDEX_NAME, "_doc", docDatas);
        esServ.flush();
    }

    @Order(6)
    @Test
    void asyncBulkUpsert4() throws IOException {
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 40));
        doc.addField(new FieldData("name", "姓名"));
        esServ.upsrt(INDEX_NAME, "_doc", doc);
        esServ.flush();
    }

    @Order(10)
    @Test
    void getDoc() {
        DocData doc = esServ.getDoc(INDEX_NAME, "_doc", "100000001", new String[]{"age", "bal", "load_bal", "nested_test.prd", "nested_test.bal"});
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(40, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));

    }

    @Order(11)
    @Test
    void getDoc2() {
        DocData doc = esServ.getDoc(INDEX_NAME, "_doc", "100000001", null);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(40, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));
        Assertions.assertEquals("姓名", doc.getValStr("name"));
    }

    @Order(12)
    @Test
    void getDoc3() {
        DocData doc = esServ.getDoc(INDEX_NAME, "_doc", "100000002", new String[]{"age", "bal", "load_bal", "nested_test", "nested_test2"});
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000002", doc.getId());

    }

    @Order(20)
    @Test
    void count() {
        esServ.flush();
        Assertions.assertEquals(20, esServ.count(INDEX_NAME, "_doc", null));
    }

    @Order(21)
    @Test
    void count02() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertEquals(1, esServ.count(INDEX_NAME, "_doc", paramMap));
    }

    @Order(21)
    @Test
    void count03() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertEquals(1, esServ.count(INDEX_NAME, "_doc", paramMap));
    }

    @Order(21)
    @Test
    void count04() {
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        Assertions.assertEquals(1, esServ.count(INDEX_NAME, "_doc", paramMap));
    }

    @Order(25)
    @Test
    void query() {
        esServ.flush();
        String sql = "select age,count(1) as ct from index_20201101 where age = 20 group by age";
        List<Map<String, Object>> list = esSqlServ.query(sql);
        Assertions.assertNotNull(list);
        Assertions.assertTrue(list.size() > 0);
        list.forEach(map -> {
            Assertions.assertEquals(2, map.size());
            Assertions.assertEquals(1, map.get("ct"));
            Assertions.assertEquals(20, map.get("age"));
        });
    }

    @Order(30)
    @Test
    void delByQuery() {
        Assertions.assertEquals("100000001", esServ.getDoc(INDEX_NAME, "_doc", "100000001", null).getId());
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.putIfAbsent("load_bal", null);
        paramMap.putIfAbsent("cid", "100000001");
        Assertions.assertTrue(esServ.delByQuery(INDEX_NAME, "_doc", paramMap));
        TimeUtil.sleepSec(WAIT_FLUSH_SEC);
        DocData doc = esServ.getDoc(INDEX_NAME, "_doc", "100000001", null);
        Assertions.assertNull(doc.getId());
    }

    @Order(35)
    @Test
    void updatIndxAlias() {
        ArrayList<String> alias = new ArrayList<>();
        alias.add("index_a");
        alias.add("index_b");
        Assertions.assertTrue(esServ.updatIndxAlias(INDEX_NAME, alias, null));
    }

    @Order(36)
    @Test
    void getAliasByIndex() {
        Set<String> alias = esServ.getAliasByIndex(INDEX_NAME);
        alias.forEach(a -> Assertions.assertTrue(a.equals("index_a") || a.equals("index_b")));
    }

    @Order(37)
    @Test
    void updatIndxAlias02() {
        ArrayList<String> alias = new ArrayList<>();
        alias.add("index_a");
        Assertions.assertTrue(esServ.updatIndxAlias(INDEX_NAME, null, alias));
        esServ.getAliasByIndex(INDEX_NAME).forEach(a -> Assertions.assertTrue(a.equals("index_b")));
    }

    @Order(38)
    @Test
    void getIndexsByAlias() {
        Set<String> indexs = esServ.getIndexsByAlias("index_b");
        Assertions.assertNotNull(indexs);
        Assertions.assertFalse(indexs.isEmpty());
        indexs.forEach(e -> Assertions.assertEquals(INDEX_NAME, e));
    }

    @Order(38)
    @Test
    void existAlias() {
        Assertions.assertTrue(esServ.existAlias(INDEX_NAME, "index_b"));
    }

    @Order(39)
    @Test
    void asyBulkDelDoc() {
        Assertions.assertEquals(2, esSqlServ.query("select count(1) as ct from index_20201101 where cid in ('100000002','100000003')").get(0).get("ct"));
        esSqlServ.asyBulkDelDoc(INDEX_NAME, "_doc", new ArrayList() {{
            add("100000002");
            add("100000003");
        }});
        esSqlServ.flush();
        Assertions.assertEquals(0, esSqlServ.query("select count(1) as ct from index_20201101 where cid in ('100000002','100000003')").get(0).get("ct"));
    }

    @Order(50)
    @Test
    void asyncBulkUpsert5() {
        ArrayList<DocData> docDatas = new ArrayList<>();
        for (int i = 1; i < 20001; i++) {
            DocData doc = new DocData();
            String cid = "20000000" + i;
            doc.setId(cid);
            doc.addField(new FieldData("cid", cid));
            doc.addField(new FieldData("age", i));
            doc.addField(new FieldData("bal", i * 1.5));
            doc.addField(new FieldData("load_bal", 3d));
            doc.addField(new FieldData("name", "姓名" + i));
            JSONArray nestedTestVals = new JSONArray();
            JSONObject val1 = new JSONObject();
            val1.put("prd", "hhh");
            val1.put("bal", 2000);
            nestedTestVals.add(val1);
            JSONObject val2 = new JSONObject();
            val2.put("prd", "hhh2");
            val2.put("bal", 50.55);
            nestedTestVals.add(val2);
            doc.addField(new FieldData("nested_test", nestedTestVals));
            docDatas.add(doc);
        }
        esServ.asyncBulkUpsert(INDEX_NAME, "_doc", docDatas);
        esServ.flush();
        Assertions.assertEquals(20000, esSqlServ.query("select count(1) as ct from index_20201101 where cid  > 200000000").get(0).get("ct"));
    }

    @Order(998)
    @Test
    void deleIndex() {
        Assertions.assertTrue(esServ.deleIndex(INDEX_NAME));
    }

    @Order(999)
    @Test
    void close() {
        esServ.close();
    }


}
