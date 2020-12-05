package com.codecubic.dao;

import com.codecubic.common.*;
import com.codecubic.exception.ESInitException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.List;

@Slf4j
class ElasticSearchServiceTest {

    private ElasticSearchService esServ;

    {
        Yaml yaml = new Yaml();
        ESConfig esConfig = yaml.loadAs(ElasticSearchService.class.getClassLoader().getResourceAsStream("application.yml"), ESConfig.class);
        try {
            esServ = new BaseElasticSearchDataSource(esConfig);
        } catch (ESInitException e) {
            e.printStackTrace();
        }
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
    void indexSchema() {
        IndexInfo indexInfo = esServ.indexSchema("index_20201101", "_doc");
        Assertions.assertEquals("index_20201101", indexInfo.getName());
        Assertions.assertEquals("_doc", indexInfo.getType());
        ArrayList<String> names = new ArrayList<>();
        names.add("cid");
        names.add("age");
        names.add("bal");
        names.add("load_bal");
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
        for (int i = 2; i < 20; i++) {
            DocData doc = new DocData();
            String cid = "10000000" + i;
            doc.setId(cid);
            doc.addField(new FieldData("cid", cid));
            doc.addField(new FieldData("age", i));
            doc.addField(new FieldData("bal", i * 1.5));
            doc.addField(new FieldData("load_bal", 3));
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
        doc.addField(new FieldData("load_bal", 20d));
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
        Assertions.assertEquals(20, doc.getValDouble("load_bal"));
    }
    @Order(11)
    @Test
    void getDoc2() {
        DocData doc = esServ.getDoc("index_20201101", "_doc", "100000001", null);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("100000001", doc.getId());
        Assertions.assertEquals(20, doc.getValInt("age"));
        Assertions.assertEquals(20, doc.getValDouble("bal"));
        Assertions.assertEquals(20, doc.getValDouble("load_bal"));
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
