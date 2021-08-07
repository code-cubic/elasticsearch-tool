package other;

import com.codecubic.common.ESConfig;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.concurrent.TimeUnit;

@Slf4j
public class BenchMarkHYTest {

    static class ReaderTP implements Runnable {

        private int start;
        private int end;
        private boolean useScore;

        public ReaderTP(boolean useScore, int start, int end) {
            this.start = start;
            this.end = end;
            this.useScore = useScore;
        }

        @SneakyThrows
        @Override
        public void run() {
            //preference=_only_nodes:kZt7TSMOSZiuUKSRsj8fBA,kmhm3RNwS_aPJmZnQySkQw
            ESConfig esConfig = new ESConfig();
            esConfig.setHttpHostInfo("10.4.86.14:9200,10.4.98.225:9202");
//            esConfig.setHttpHostInfo("10.4.98.225:9202");
            IESDataSource esServ = new ESDataSource(esConfig);
            long max = 0;
            long sum = 0;
            long oversize = 0;
            long gStart = System.currentTimeMillis();
            for (int i = this.start; i < this.end; i++) {
                long start = System.currentTimeMillis();
//                esServ.getDoc("cpp_test_wh_tp_ap_20210305", "_doc", "cid_" + i, new String[]{"etl_dt", "name", "hhh_bal"});
                SearchRequest req = new SearchRequest("cpp_test_wh_tp_ap_20210305");
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//                req.preference("_local");
                req.types("_doc");
                BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
                if (this.useScore) {
                    boolBuilder.must(QueryBuilders.existsQuery("etl_dt"));
                    boolBuilder.must(QueryBuilders.termsQuery("customer_id", "cid_" + RandomUtils.nextInt(0, 5000000), "cid_" + RandomUtils.nextInt(0, 5000000)));
                } else {
                    boolBuilder.must(QueryBuilders.constantScoreQuery(QueryBuilders.existsQuery("etl_dt")));
                    boolBuilder.must(QueryBuilders.constantScoreQuery(QueryBuilders.termsQuery("customer_id", "cid_" + RandomUtils.nextInt(0, 5000000), "cid_" + RandomUtils.nextInt(0, 5000000))));
                }
                sourceBuilder.query(boolBuilder);
//                sourceBuilder.aggregation(AggregationBuilders.terms("cnt").field("etl_dt"));

                //最多返回100条
                sourceBuilder.size(2);
                //设置最多60秒超时
                sourceBuilder.timeout(new TimeValue(5000, TimeUnit.MILLISECONDS));
                //设置查询范围，只查用户输入的字段
                sourceBuilder.fetchSource(new String[]{
                        "customer_id", "lab_addr", "hhh_bal3.bal"
                }, null);

                req.source(sourceBuilder).searchType(SearchType.DEFAULT);
//                req.preference("_only_nodes:kZt7TSMOSZiuUKSRsj8fBA,kmhm3RNwS_aPJmZnQySkQw");
                req.preference("_only_nodes:kmhm3RNwS_aPJmZnQySkQw,kZt7TSMOSZiuUKSRsj8fBA");
                SearchHits hits = esServ.getClient().search(req, RequestOptions.DEFAULT).getHits();
                long diff = System.currentTimeMillis() - start;
                max = max < diff ? diff : max;
                sum += diff;
                if (diff > 900) {
                    oversize++;
                }
            }
            esServ.close();

            log.info("max={},avg={},oversize={},allElapsed={}", max, sum / (this.end - this.start), oversize, System.currentTimeMillis() - gStart);
        }
    }

    static class ReaderAP implements Runnable {

        private int start;
        private int end;

        public ReaderAP(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @SneakyThrows
        @Override
        public void run() {
            ESConfig esConfig = new ESConfig();
            esConfig.setHttpHostInfo("10.4.86.12:9200,10.4.86.13:9200");
            IESDataSource esServ = new ESDataSource(esConfig);
            long max = 0;
            long sum = 0;
            long oversize = 0;
            long gStart = System.currentTimeMillis();
            for (int i = this.start; i < this.end; i++) {
                long start = System.currentTimeMillis();
                SearchRequest req = new SearchRequest("cpp_test_wh_tp_ap_20210305");
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//                req.preference("_local");
                req.types("_doc");
//                BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
//                sourceBuilder.query(boolBuilder);
                sourceBuilder.aggregation(AggregationBuilders.terms("cnt").field("etl_dt"));

                //最多返回100条
                sourceBuilder.size(2);
                //设置最多60秒超时
                sourceBuilder.timeout(new TimeValue(5000, TimeUnit.MILLISECONDS));
                //设置查询范围，只查用户输入的字段
                sourceBuilder.fetchSource(new String[]{
                        "customer_id", "lab_addr", "cnt"
                }, null);

                req.source(sourceBuilder).searchType(SearchType.DEFAULT);
                req.preference("_only_nodes:VueKwllUT8umbVAmAmXdmw,MUneslG5RqCJ9yx5EMgV0w");
                SearchResponse search = esServ.getClient().search(req, RequestOptions.DEFAULT);
                Aggregations aggregations = search.getAggregations();
                Aggregation cnt = aggregations.get("cnt");
                SearchHits hits = search.getHits();
                long diff = System.currentTimeMillis() - start;
                max = max < diff ? diff : max;
                sum += diff;
                if (diff > 900) {
                    oversize++;
                }
            }
            esServ.close();

            log.info("max={},avg={},oversize={},allElapsed={}", max, sum / (this.end - this.start), oversize, System.currentTimeMillis() - gStart);
        }
    }


    public static void main(String[] args) throws InterruptedException {



        int parrll = Integer.parseInt(args[0]);
        int start = Integer.parseInt(args[1]);
        int step = Integer.parseInt(args[2]);
        log.info("parrll={}", parrll);
        log.info("start={}", start);
        log.info("step={}", step);

        int endOffset;
        for (int i = 0; i < parrll; i++) {
            endOffset = start + step;
            new Thread(new ReaderAP(start, endOffset)).start();
            start = endOffset;
        }

        Thread.currentThread().join(1000000);
    }
}
