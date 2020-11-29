package com.codecubic.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.ESConfig;
import com.codecubic.exception.ESInitException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticSearchDataSource extends BaseElasticSearchDataSource {


    public ElasticSearchDataSource(ESConfig config) throws ESInitException {
        super(config);
    }

    public List<Map<String, Object>> query(String sql) {
        String esRootHost = "";
        StringBuffer sb = new StringBuffer();
        String esRestUri = sb.append("http://").append(esRootHost).append("/_xpack/sql?format=json").toString();
        JSONObject post_data = new JSONObject();
        post_data.put("query", sql);
        HttpClientHandler hander = new HttpClientHandler();
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(this._esConf.getSocketTimeoutMillis())
                .setConnectTimeout(this._esConf.getConnectTimeoutMillis()).build();
        HttpClientHandler.HandlerResponse response = null;

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            response = hander.httpPostDemo(esRestUri, post_data.toString(), requestConfig, ContentType.APPLICATION_JSON, null);

            JSONObject responseJson = JSONObject.parseObject(response.getContent());
            JSONArray columns = responseJson.getJSONArray("columns");
            JSONArray rows = responseJson.getJSONArray("rows");

            if (rows != null) {
                for (Object row : rows) {
                    if (row instanceof JSONArray) {
                        JSONArray array = (JSONArray) row;
                        JSONObject record = new JSONObject();
                        for (int i = 0; i < columns.size(); i++) {
                            JSONObject column = columns.getJSONObject(i);
                            String keyName = column.getString("name");
                            record.put(keyName, array.get(i));
                        }
                        result.add(record.getInnerMap());
                    } else {
                        continue;
                    }
                }
                return result;
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return new ArrayList<>(0);
    }
}
