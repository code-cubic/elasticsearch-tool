package com.codecubic.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.*;
import com.codecubic.exception.BulkProcessorInitExcp;
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


    static {
        Yaml yaml = new Yaml();
        ESConfig esConfig = yaml.loadAs(IESDataSource.class.getClassLoader().getResourceAsStream("application.yml"), ESConfig.class);
        try {
            esServ = new BaseESDataSource(esConfig);
            esSqlServ = new ESDataSource(esConfig);
        } catch (ESCliInitExcep e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
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
        FieldInfo nestedTest = new FieldInfo("nested_test", "nested");
        nestedTest.addFields(new FieldInfo("prd", "keyword"));
        nestedTest.addFields(new FieldInfo("bal", "double"));
        prop.addFields(nestedTest);
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
        FieldInfo nestedTest = new FieldInfo("nested_test2", "nested");
        nestedTest.addFields(new FieldInfo("prd", "keyword"));
        nestedTest.addFields(new FieldInfo("bal", "double"));
        prop.addFields(nestedTest);
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
        IndexInfo indexInfo = esServ.getIndexSchema("index_20201101", "_doc");
        Assertions.assertEquals("index_20201101", indexInfo.getName());
        Assertions.assertEquals("_doc", indexInfo.getType());
    }

    @Order(4)
    @Test
    void asyncUpsert() throws IOException {
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 10));
        esServ.upsrt("index_20201101", "_doc", doc);
    }

    @Order(4)
    @Test
    void asyncBulkUpsert2() throws BulkProcessorInitExcp {
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
        esServ.asyncBulkUpsert("index_20201101", "_doc", docDatas);
        TimeUtil.sleepSec(3);
    }

    @Order(5)
    @Test
    void asyncBulkUpsert3() throws BulkProcessorInitExcp {
        ArrayList<DocData> docDatas = new ArrayList<>();
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 20));
        doc.addField(new FieldData("bal", 20d));
        doc.addField(new FieldData("name", "姓名"));
        docDatas.add(doc);
        esServ.asyncBulkUpsert("index_20201101", "_doc", docDatas);
        TimeUtil.sleepSec(3);
    }

    @Order(6)
    @Test
    void asyncBulkUpsert4() throws IOException {
        DocData doc = new DocData();
        doc.setId("100000001");
        doc.addField(new FieldData("cid", "100000001"));
        doc.addField(new FieldData("age", 40));
        doc.addField(new FieldData("name", "姓名"));
        esServ.upsrt("index_20201101", "_doc", doc);
        TimeUtil.sleepSec(3);
    }

    @Order(10)
    @Test
    void getDoc() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", new String[]{"age", "bal", "load_bal", "nested_test.prd", "nested_test.bal"});
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(40, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));

    }

    @Order(11)
    @Test
    void getDoc2() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", null);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(40, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));
        Assertions.assertEquals("姓名", doc.getValStr("name"));
    }

    @Order(12)
    @Test
    void getDoc3() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000002", new String[]{"age", "bal", "load_bal", "nested_test", "nested_test2"});
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000002", doc.getId());

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
    void delByQuery() throws BulkProcessorInitExcp {
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

    @Order(39)
    @Test
    void asyBulkDelDoc() throws BulkProcessorInitExcp {
        Assertions.assertEquals(2, esSqlServ.query("select count(1) as ct from index_20201101 where cid in ('100000002','100000003')").get(0).get("ct"));
        esServ.asyBulkDelDoc("index_20201101", "_doc", new ArrayList() {{
            add("100000002");
            add("100000003");
        }});
        TimeUtil.sleepSec(5);
        Assertions.assertEquals(0, esSqlServ.query("select count(1) as ct from index_20201101 where cid in ('100000002','100000003')").get(0).get("ct"));
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
