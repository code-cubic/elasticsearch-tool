package other;

import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.DocData;
import com.codecubic.common.ESConfig;
import com.codecubic.common.FieldData;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import com.codecubic.exception.ESCliInitExcep;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BenchMarkTest {
    static class Writer implements Runnable {
        private int start;
        private int end;
        private ESDataSource esServ;

        public Writer(int start, int end, ESDataSource serv) {
            this.start = start;
            this.end = end;
            this.esServ = serv;
        }

        @SneakyThrows
        @Override
        public void run() {


            long start = System.currentTimeMillis();
            for (int i = this.start; i < this.end; i++) {
                DocData docData = new DocData();
                docData.addField(new FieldData("customer_id", "cid_" + i));
                docData.addField(new FieldData("etl_dt", "20210305"));
                docData.addField(new FieldData("lab_double", i * 100.0d));
                docData.addField(new FieldData("lab_int", i * 10));
                docData.addField(new FieldData("lab_long", i * 10000));
                docData.addField(new FieldData("lab_addr", "北京市xxx区xxx楼" + i));
                docData.addField(new FieldData("lab_ggz", i % 2 == 0 ? "是" : "否"));
                docData.addField(new FieldData("lab_bmd", i % 2 == 0 ? "1" : "0"));
                docData.addField(new FieldData("lab_hmd", i % 2 == 0 ? "1" : "0"));
                docData.addField(new FieldData("name", "XXXMMMDDDD" + i));
                JSONObject balJson = new JSONObject();
                balJson.put("prd", "好会花_" + i);
                balJson.put("stage", "stage_" + i);
                balJson.put("bal", i * 10);
//                docData.addField(new FieldData("hhh_bal", balJson));
//                docData.addField(new FieldData("hhh_bal2", balJson));
//                docData.addField(new FieldData("hhh_bal3", balJson));
//                docData.addField(new FieldData("hhh_bal4", balJson));
//                docData.addField(new FieldData("hhh_bal5", balJson));
                docData.setId("cid_" + i);
//                esServ.asyncBulkUpsert("cpp_test_wh_tp_ap_20210305", "_doc", docData);
            }

            log.info("{},{},elapsed:{}(ms)", this.start, end, System.currentTimeMillis() - start);
        }
    }

    public static void main(String[] args) throws NoSuchFieldException, ESCliInitExcep {

        int parrll = Integer.parseInt(args[0]);
        int start = Integer.parseInt(args[1]);
        int step = Integer.parseInt(args[2]);
        log.info("parrll={}", parrll);
        log.info("start={}", start);
        log.info("step={}", step);
        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        esConfig.setReqWriteWaitMill(2000);
        esConfig.setBackOffRetries(1);
        esConfig.setBackOffSec(1L);

        IESDataSource esServ = new ESDataSource(esConfig);

        int endOffset;
        for (int i = 0; i < parrll; i++) {
            endOffset = start + step;
//            new Thread(new Writer(start, endOffset, esServ)).start();
            start = endOffset;
        }

        esServ.close();


    }
}


