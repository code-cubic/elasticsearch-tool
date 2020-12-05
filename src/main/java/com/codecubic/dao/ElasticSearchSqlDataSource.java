package com.codecubic.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.ESConfig;
import com.codecubic.exception.ESInitException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 支持sql查询，所有sql查询方法均为非稳定版本
 *
 * @author code-cubic
 */
@Slf4j
public class ElasticSearchSqlDataSource extends BaseElasticSearchDataSource {

    private HttpClientHandler _sqlHandler;

    public ElasticSearchSqlDataSource(ESConfig config) throws ESInitException {
        this._esConf = config;
        String[] hosts = StringUtils.split(this._esConf.getHttpHostInfo(), ",");
        String[] split = StringUtils.split(hosts[0], ":");
        String url = String.format("http://%s:%s/_xpack/sql?format=json", split[0], split[1]);
        RequestConfig conf = RequestConfig.custom()
                .setSocketTimeout(this._esConf.getSocketTimeoutMillis())
                .setConnectTimeout(this._esConf.getConnectTimeoutMillis()).build();
        _sqlHandler = new HttpClientHandler(url, conf);
    }

    public List<Map<String, Object>> query(String sql) {
        JSONObject json = new JSONObject();
        json.put("query", sql);

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            HttpClientHandler.HandlerResponse response = _sqlHandler.httpPostDemo(json.toString(), ContentType.APPLICATION_JSON, null);

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
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return result;
    }
}
