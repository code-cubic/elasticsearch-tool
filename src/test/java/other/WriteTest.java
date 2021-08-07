package other;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.DocData;
import com.codecubic.common.ESConfig;
import com.codecubic.common.FieldData;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import com.codecubic.exception.ESCliInitExcep;

public class WriteTest {

    /**
     * test20210219
     * "customer_id" : {
     * "type" : "keyword"
     * },
     * "etl_dt" : {
     * "type" : "keyword"
     * },
     * "hhh_bal": {
     * "type": "nested",
     * "properties":{
     * "prd":{ "type":"keyword"},
     * "stage":{  "type":"keyword"},
     * "bal":{  "type":"double"}
     * }
     * }
     * 普通情况写
     *
     * @param args
     */
    public static void main1(String[] args) throws ESCliInitExcep, NoSuchFieldException {

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        esConfig.setBufferWriteSize(20L);
        esConfig.setIoThreadCount(4);
//        esConfig.setBatch(50);

        IESDataSource esServ = new ESDataSource(esConfig);

        for (int i = 14200000; i < 16200000; i++) {
            DocData docData = new DocData();
            FieldData cid = new FieldData("customer_id", "cid_" + i);
            FieldData etl = new FieldData("etl_dt", "20210220");
            JSONObject balJson = new JSONObject();
            balJson.put("prd", "好会花-" + i);
            balJson.put("stage", "stage-" + i);
            balJson.put("bal", i);
            FieldData hhhBal = new FieldData("hhh_bal", balJson);
            docData.addField(cid);
            docData.addField(etl);
            docData.addField(hhhBal);
            docData.setId(cid.getVal().toString());
//            esServ.asyncUpsert("test20210219", "_doc", docData);
        }
        esServ.close();
    }


    /**
     * 取消评分
     * put test20210219
     * {
     * "mappings" : {
     * "_doc" : {
     * "properties" : {
     * "customer_id" : {
     * "type" : "keyword",
     * "norms": false
     * },
     * "etl_dt" : {
     * "type" : "keyword",
     * "norms": false
     * },
     * "hhh_bal": {
     * "type": "nested",
     * "norms": false,
     * "properties":{
     * "prd":{ "type":"keyword","norms": false},
     * "stage":{  "type":"keyword","norms": false},
     * "bal":{  "type":"double","norms": false}
     * }
     * }
     * }
     * }
     * },
     * "settings" : {
     * "index" : {
     * "refresh_interval" : "3s",
     * "number_of_shards" : "2",
     * "number_of_replicas" : "0"
     * }
     * }
     * }
     */


    public static void main4(String[] args) throws ESCliInitExcep, NoSuchFieldException {

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        esConfig.setBufferWriteSize(25L);
        esConfig.setIoThreadCount(4);
//        esConfig.setBatch(50);

        IESDataSource esServ = new ESDataSource(esConfig);

        for (int i = 1000000; i < 2000000; i++) {
            DocData docData = new DocData();
            FieldData cid = new FieldData("customer_id", "cid_" + i);
            FieldData etl = new FieldData("etl_dt", "20210220");
            JSONObject balJson = new JSONObject();
            balJson.put("prd", "好会花2-" + i);
            balJson.put("stage", "stage3-" + i);
            balJson.put("bal", i);
            docData.addField(cid);
            docData.addField(etl);
            docData.addField(new FieldData("hhh_bal", balJson));
            docData.addField(new FieldData("hhh_bal2", balJson));
            docData.addField(new FieldData("hhh_bal3", balJson));
            docData.addField(new FieldData("hhh_bal4", balJson));
            docData.addField(new FieldData("hhh_bal5", balJson));
            docData.addField(new FieldData("hhh_bal6", balJson));
            docData.addField(new FieldData("hhh_bal7", balJson));
            docData.addField(new FieldData("hhh_bal8", balJson));
            docData.setId(cid.getVal().toString());
//            esServ.asyncUpsert("test20210218", "_doc", docData);
        }
        esServ.close();
    }


    public static void main3(String[] args) throws ESCliInitExcep, NoSuchFieldException {

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        esConfig.setBufferWriteSize(20L);
        esConfig.setIoThreadCount(4);

        IESDataSource esServ = new ESDataSource(esConfig);

        for (int i = 0; i < 100000; i++) {
            DocData docData = new DocData();
            FieldData cid = new FieldData("customer_id", "cid_" + i);
            FieldData etl = new FieldData("etl_dt", "20210220");
            docData.addField(cid);
            docData.addField(etl);
            docData.addField(new FieldData("prd", "好会花2-" + i));
            docData.addField(new FieldData("stage", "stage3-" + i));
            docData.addField(new FieldData("bal", i));
            docData.addField(new FieldData("prd2", "好会花2-" + i));
            docData.addField(new FieldData("stage2", "stage3-" + i));
            docData.addField(new FieldData("bal2", i));
            docData.addField(new FieldData("prd3", "好会花2-" + i));
            docData.addField(new FieldData("stage3", "stage3-" + i));
            docData.addField(new FieldData("bal3", i));
            docData.addField(new FieldData("prd4", "好会花2-" + i));
            docData.addField(new FieldData("stage4", "stage3-" + i));
            docData.addField(new FieldData("bal4", i));
            docData.setId(cid.getVal().toString());
//            esServ.asyncUpsert("test20210117", "_doc", docData);
        }
        esServ.close();
    }


    public static void main(String[] args) throws ESCliInitExcep, NoSuchFieldException {

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        esConfig.setBufferWriteSize(15L);
//        esConfig.setIoThreadCount(6);

        IESDataSource esServ = new ESDataSource(esConfig);

        for (int i = 5800000; i < 6800000; i++) {
            DocData docData = new DocData();
            FieldData cid = new FieldData("customer_id", "cid_" + i);
            FieldData etl = new FieldData("etl_dt", "20210220");
            for (int j = 0; j < 100; j++) {
                docData.addField(new FieldData("" + j, "20210220"));
            }
            JSONArray arr = new JSONArray();
            JSONObject balJson1 = new JSONObject();
            balJson1.put("prd", "好会花-" + i);
            balJson1.put("stage", "stage-" + i);
            balJson1.put("bal", i);
            arr.add(balJson1);
            JSONObject balJson2 = new JSONObject();
            balJson2.put("prd", "好会花2-" + i);
            balJson2.put("stage", "stage2-" + i);
            balJson2.put("bal", i);
            arr.add(balJson2);
            FieldData hhhBal = new FieldData("hhh_bal", arr);
            docData.addField(cid);
            docData.addField(etl);
            docData.addField(hhhBal);
            docData.setId(cid.getVal().toString());
//            esServ.asyncUpsert("test_20210301", "_doc", docData);
        }
        esServ.close();
    }


}
