package com.codecubic.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codecubic.common.ESConfig;
import com.codecubic.common.annotation.SnapShot;
import com.codecubic.exception.ESCliInitExcep;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * support sql query
 * snap version
 *
 * @author code-cubic
 */
@Slf4j
@SnapShot
public class ESDataSource extends BaseESDataSource {

    private HttpClientHandler sqlHandler;

    public ESDataSource(ESConfig config) throws ESCliInitExcep {
        super(config);
        String[] hosts = StringUtils.split(super.esConf.getHttpHostInfo(), ",");
        String[] split = StringUtils.split(hosts[0], ":");
        String url = String.format("http://%s:%s/_xpack/sql", split[0], split[1]);
        RequestConfig conf = RequestConfig.custom()
                .setSocketTimeout(super.esConf.getSocketTimeoutMillis())
                .setConnectTimeout(super.esConf.getConnectTimeoutMillis()).build();
        this.sqlHandler = new HttpClientHandler(url, conf);
    }


    @Override
    public List<Map<String, Object>> query(String sql) {

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Preconditions.checkNotNull(sql, "sql can not be null");
            JSONObject json = new JSONObject();
            json.put("query", sql);
            HttpClientHandler.HandlerResponse response = this.sqlHandler.httpPostDemo(json.toString(), ContentType.APPLICATION_JSON, null);

            JSONObject responseJson = JSON.parseObject(response.getContent());
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
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return result;
    }
}
