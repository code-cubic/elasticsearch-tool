package other;

import com.codecubic.common.ESConfig;
import com.codecubic.dao.ESDataSource;
import com.codecubic.dao.IESDataSource;
import com.codecubic.exception.ESCliInitExcep;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SearchTest {
    public static void main(String[] args) throws IOException, ESCliInitExcep, NoSuchFieldException {
        SearchRequest req = new SearchRequest("test20210218");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        req.preference("_local");
        req.types("_doc");
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(QueryBuilders.existsQuery("customer_id"));

        sourceBuilder.query(boolBuilder);
        sourceBuilder.aggregation(AggregationBuilders.terms("cnt").field("etl_dt"));

        //最多返回100条
        sourceBuilder.size(100);
        //设置最多60秒超时
        sourceBuilder.timeout(new TimeValue(5000, TimeUnit.MILLISECONDS));
        //设置查询范围，只查用户输入的字段
        sourceBuilder.fetchSource(new String[]{
                "customer_id", "cnt"
        }, null);

        req.source(sourceBuilder).searchType(SearchType.DEFAULT);

        ESConfig esConfig = new ESConfig();
        esConfig.setHttpHostInfo("192.168.99.10:9200");
        IESDataSource esServ = new ESDataSource(esConfig);
        RestHighLevelClient client = esServ.getClient();


        client.search(req, RequestOptions.DEFAULT).getHits().iterator().forEachRemaining(hit -> System.out.println(hit));
        esServ.close();
    }
}
