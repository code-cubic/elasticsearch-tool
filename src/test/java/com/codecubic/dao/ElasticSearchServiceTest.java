package com.codecubic.dao;

import com.codecubic.common.ESConfig;
import com.codecubic.exception.ESInitException;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.core.util.Assert;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.List;

@Slf4j
class ElasticSearchServiceTest {

    private ElasticSearchService esServ;

    {
        Yaml yaml = new Yaml();
        ESConfig esConfig = yaml.loadAs(ElasticSearchService.class.getClassLoader().getResourceAsStream("application.yml"), ESConfig.class);
        try {
            esServ = new BaseElasticSearchDataSource(esConfig);
        } catch (ESInitException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getAllIndex() {
        List<String> allIndex = esServ.getAllIndex();
        Assert.isNonEmpty(allIndex);
        allIndex.forEach(index -> System.out.println(index));
    }
}
