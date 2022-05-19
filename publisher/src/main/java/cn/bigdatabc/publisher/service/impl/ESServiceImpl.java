package cn.bigdatabc.publisher.service.impl;

import cn.bigdatabc.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//将当前对象的创建交给Spring容器进行管理
@Service
public class ESServiceImpl implements ESService {

    //将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;

    /*
    GET /gmall0523_dau_info_2020-10-24-query/_search
    {
      "query": {
        "match_all": {}
      }
    }
     */
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        String query = sourceBuilder.toString();
        String indexName = "gmall0523_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        Long total = 0L;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }

        return total;
    }

    /*
    GET /gmall0523_dau_info_2020-10-24-query/_search
    {
      "aggs": {
        "groupBy_hr": {
          "terms": {
            "field": "hr",
            "size": 24
          }
        }
      }
    }

     */
    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> hourMap = new HashMap<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder =
                new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);

        sourceBuilder.aggregation(termsAggregationBuilder);

        String indexName = "gmall0523_dau_info_"+ date +"-query";
        String query = sourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation termsAgg = result.getAggregations().getTermsAggregation("groupBy_hr");
            if(termsAgg!=null){
                List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(),bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return hourMap;
    }
}
