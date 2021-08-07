package other;

import com.codecubic.common.ESConfig;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import com.codecubic.exception.ESCliInitExcep;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadTest {
    public static void main(String[] args) throws ESCliInitExcep, NoSuchFieldException {

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        IESDataSource esServ = new ESDataSource(esConfig);

        for (int i = 0; i < 100000; i++) {
            String sql = "select customer_id,etl_dt from test20210218 where customer_id is not null and etl_dt is not null limit 2";
//            log.info("sql={}", sql);
            long start = System.currentTimeMillis();
            esServ.query(sql);
            long diff = System.currentTimeMillis() - start;
            if (diff > 1000) {
                log.info("diff={}", diff);
            }

        }
    }
}
