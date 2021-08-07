package other;

import com.codecubic.common.DocData;
import com.codecubic.common.ESConfig;
import com.codecubic.common.FieldData;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import com.codecubic.exception.ESCliInitExcep;

public class WriteTst {
    public static void main(String[] args) throws ESCliInitExcep {
        ESConfig esConf = new ESConfig();
        esConf.setHttpHostInfo("192.168.99.20:9100");
        esConf.setBufferWriteSize(20L);
        IESDataSource esServ = new ESDataSource(esConf);
        for (int i = 1000000; i < 2000000; i++) {
            DocData doc = new DocData();
            doc.addField(new FieldData("name", "name" + i));
            doc.addField(new FieldData("lab_hmd", "lab_hmd" + i));
            doc.addField(new FieldData("lab_ggz", "lab_ggz" + i));
            doc.addField(new FieldData("lab_addr", "lab_addr" + i));
            doc.addField(new FieldData("lab_long", 100));
            doc.addField(new FieldData("lab_int", 100));
            doc.addField(new FieldData("lab_double", 100.0));
            doc.addField(new FieldData("etl_dt", "20210715"));
            doc.addField(new FieldData("customer_id", "cid_" + i));
            doc.setId("cid_" + i);
            esServ.asyncUpsert("test_tp_08", "_doc", doc);
        }
        esServ.flush();
        esServ.close();
    }
}
