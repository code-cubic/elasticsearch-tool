package com.codecubic.dao;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class HttpClientHandler {
    /**
     * 池化管理
     */
    private static PoolingHttpClientConnectionManager poolConnManager = null;
    /**
     * 它是线程安全的，所有的线程都可以使用它一起发送http请求
     */
    private static CloseableHttpClient httpClient;
    /**
     * 重试次数
     */
    private static int DEFAULT_MAX_RETRY = 3;
    /**
     * 最大链接并发
     */
    private static int DEFAULT_MAX_TOTAL_CONN = 1000;
    /**
     * 每个路由最大默认并发数
     */
    private static int DEFAULT_MAX_PER_ROUTE = 400;
    /**
     * 从连接池获取连接的timeout最大等待时间(ms)
     */
    private static int DEFAULT_CONN_REQ_TIMEOUT = 1000;
    /**
     * 建立链接的timeout(ms)
     */
    private static int DEFAULT_CONN_TIMEOUT = 100;
    /**
     * 指客户端从服务器读取数据的timeout (ms)，超出后会抛出SocketTimeOutException
     */
    private static int DEFAULT_SOCKET_TIMEOUT = 20000;
    /**
     * 针对网络和超时等错误以及其他远程服务定义的基于http状态码返回的错误
     */
    public static final String remoteRequestError = "远程服务返回错误码";

    private static Properties properties = new Properties();

    static {
        InputStream input = null;
        try {
            input = HttpClientHandler.class.getClassLoader().getResourceAsStream("dispatch.properties");
            if (input == null) {
                log.warn("can not found dispatch.properties,use default settings http hander");
            } else {
                properties.load(input);
                DEFAULT_MAX_TOTAL_CONN = Integer.parseInt(properties.getProperty("MAX_TOTAL_CONN", Integer.toString(DEFAULT_MAX_TOTAL_CONN)));
                DEFAULT_MAX_PER_ROUTE = Integer.parseInt(properties.getProperty("MAX_PER_ROUTE_CONN", Integer.toString(DEFAULT_MAX_PER_ROUTE)));
                DEFAULT_MAX_RETRY = Integer.parseInt(properties.getProperty("MAX_RETRY", Integer.toString(DEFAULT_MAX_RETRY)));
                DEFAULT_CONN_REQ_TIMEOUT = Integer.parseInt(properties.getProperty("DEFAULT_CONN_REQ_TIMEOUT", Integer.toString(DEFAULT_CONN_REQ_TIMEOUT)));
                DEFAULT_CONN_TIMEOUT = Integer.parseInt(properties.getProperty("DEFAULT_CONN_TIMEOUT", Integer.toString(DEFAULT_CONN_TIMEOUT)));
                DEFAULT_SOCKET_TIMEOUT = Integer.parseInt(properties.getProperty("DEFAULT_SOCKET_TIMEOUT", Integer.toString(DEFAULT_SOCKET_TIMEOUT)));
                input.close();
            }
        } catch (Exception e) {
            log.error("http config init error, use default settings http hander使用默认设置:", e);
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
            }
        }
    }

    static {
        try {
            log.info("初始化HttpClient~~~开始");
            LayeredConnectionSocketFactory sslsf;
            sslsf = new SSLConnectionSocketFactory(SSLContext.getDefault());
            // 配置同时支持 HTTP 和 HTPPS
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("http", PlainConnectionSocketFactory.getSocketFactory()).register("https", sslsf).build();
            // 初始化连接管理器
            poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // 同时最多连接数
            poolConnManager.setMaxTotal(DEFAULT_MAX_TOTAL_CONN);
            // 设置每个路由最大并发数,因为只有一个aiquery,所以路由并发等于池并发数
            poolConnManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
            httpClient = getConnection();
            log.info("初始化HttpClient~~~结束");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取client
     *
     * @return
     */
    private static CloseableHttpClient getConnection() {

        log.info("DEFAULT_MAX_RETRY:{}", DEFAULT_MAX_RETRY);
        log.info("DEFAULT_MAX_TOTAL_CONN:{}", DEFAULT_MAX_TOTAL_CONN);
        log.info("DEFAULT_MAX_PER_ROUTE:{}", DEFAULT_MAX_PER_ROUTE);
        log.info("DEFAULT_CONN_REQ_TIMEOUT(ms){}", DEFAULT_CONN_REQ_TIMEOUT);
        log.info("DEFAULT_CONN_TIMEOUT(ms):{}", DEFAULT_CONN_TIMEOUT);
        log.info("DEFAULT_SOCKET_TIMEOUT(ms):{}", DEFAULT_SOCKET_TIMEOUT);
        //setConnectTimeout 建立链接的timeout
        //setConnectionRequestTimeout指从连接池获取连接的timeout
        //setSocketTimeout 指客户端从服务器读取数据的timeout，超出后会抛出SocketTimeOutException
        //这些是默认超时设置，每个请求可以自己单独设置自己的超时,但一般业务上一般只有tSocketTimeout需要设置，连接建立超时和连接获取超时偏技术层面，属于总体性能优化的考虑，一般是固定全局的
        RequestConfig config = RequestConfig.custom().setConnectTimeout(1000).setConnectionRequestTimeout(1000).setSocketTimeout(20 * 1000).build();

        //自定义重试类，要幂等并且重试次数小于预定值才发起重试
        HttpRequestRetryHandler retryHandler = (exception, executionCount, context) -> {
            if (executionCount >= DEFAULT_MAX_RETRY) {
                // Do not retry if over max retry count
                return false;
            }
            if (exception instanceof InterruptedIOException) {
                // Timeout
                return false;
            }
            if (exception instanceof UnknownHostException) {
                // Unknown host
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {
                // Connection refused
                return false;
            }
            if (exception instanceof SSLException) {
                // SSL handshake exception
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
            if (idempotent) {
                //请求幂等才发起重试
                return true;
            }
            return false;
        };
        CloseableHttpClient httpClient = HttpClients.custom()
                // 设置连接池管理
                .setConnectionManager(poolConnManager)
                .setDefaultRequestConfig(config)
                //禁止重试
                .disableAutomaticRetries()
//                .setRetryHandler(new DefaultHttpRequestRetryHandler(2, false))  //默认类重试
//                .setRetryHandler(retryHandler)//自定义类重试
                .build();
        return httpClient;
    }

    public enum ResultStatus {
        success,
        connect_timeout,
        read_timeout,
        remote_response_error,
        request_common_io_exception,
        default_error,
        unset
    }

    @Data
    @ToString
    public class HandlerResponse {
        private ResultStatus status;
        private String message;
        private String content;

        public HandlerResponse(String content, ResultStatus status, String message) {
            this.content = "";
            this.content = content;
            this.message = message;
            this.status = status;
        }

    }


    /**
     * @param url
     * @param requestConfig
     * @return
     */
    public HandlerResponse httpGetDemo(String url, RequestConfig requestConfig, Header[] heads) {

        String errorMessage = "";
        ResultStatus status;
        String result = null;
        HttpGet httpGet = new HttpGet(url);
        //此处设置每次请求的超时时间
        if (requestConfig != null) {
            //设置超时时间
            httpGet.setConfig(requestConfig);
        }
        if (heads != null) {
            httpGet.setHeaders(heads);
        }
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                result = EntityUtils.toString(entity);
                status = ResultStatus.success;

            } else {
                log.error("请求{}返回错误码：{}", url, code);
                errorMessage = remoteRequestError + "-" + "状态码" + code;
                status = ResultStatus.remote_response_error;
            }
        } catch (IOException e) {
            log.error("httpclient远程访问IO异常:", e);
            status = ResultStatus.request_common_io_exception;
            errorMessage = "httpclient远程访问IO异常:" + e.getMessage();
        } catch (Exception e) {
            log.error("httpclient请求过程中遇到其他非IO异常", e);
            errorMessage = "httpclient请求过程中遇到其他非IO异常:" + e.getMessage();
            status = ResultStatus.default_error;

        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error("httpclien response关闭失败异常", e);
                    errorMessage = "httpclien response关闭失败异常" + e.getMessage();
                }
            }
        }
        if (!status.equals(ResultStatus.success)) {
            errorMessage = "http客户端发起服务调用未成功，原因:" + errorMessage;
        }
        HandlerResponse handlerResponse = new HandlerResponse(result, status, errorMessage);
        return handlerResponse;
    }

    /**
     * application/json  UTF-8  发送post请求
     *
     * @param uri
     * @param postData
     * @param requestConfig 不用单独设置超时传null
     * @param contentType
     * @param heads         没有则不传，  @return
     */
    public HandlerResponse httpPostDemo(String uri, String postData, RequestConfig requestConfig, ContentType contentType, Header... heads) {
        String errorMessage = "";
        ResultStatus status;
        String result = null;
        HttpPost httpPost = new HttpPost(uri);
        CloseableHttpResponse response = null;
        try {
            StringEntity paramEntity = new StringEntity(postData, "UTF-8");
            httpPost.setEntity(paramEntity);
            paramEntity.setContentEncoding("UTF-8");
            paramEntity.setContentType(contentType.toString());
            httpPost.setEntity(paramEntity);
            //设置每次请求的超时时间
            if (requestConfig != null) {
                //设置超时时间
                httpPost.setConfig(requestConfig);
            }
            //设置header
            if (heads != null) {
                httpPost.setHeaders(heads);
            }
            response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                result = EntityUtils.toString(entity);
                status = ResultStatus.success;
            } else {
                log.error("请求{}返回错误码:{},请求参数:{}", uri, code, postData);
                errorMessage = remoteRequestError + "-" + "状态码" + code;
                status = ResultStatus.remote_response_error;
            }
            EntityUtils.consume(entity);
        } catch (ConnectTimeoutException e) {
            log.error("httpclient请求超时异常", e);
            status = ResultStatus.connect_timeout;
            errorMessage = e.getMessage() + " RestfulHandler处理遇到异常远程服务连接超时（客户端）， 超时阈值：" + requestConfig.getConnectionRequestTimeout() + "ms";

        } catch (SocketTimeoutException e) {
            log.error("httpclient请求超时异常", e);
            status = ResultStatus.read_timeout;
            errorMessage = e.getMessage() + " RestfulHandler处理遇到异常远程服务响应超时（客户端）， 超时阈值：" + requestConfig.getSocketTimeout() + "ms";
        } catch (IOException e) {
            log.error("httpclient远程访问IO异常:", e);
            status = ResultStatus.request_common_io_exception;
            errorMessage = "httpclient远程访问IO异常:" + e.getMessage();
        } catch (Exception e) {
            log.error("httpclient请求过程中遇到其他非IO异常", e);
            errorMessage = "httpclient请求过程中遇到其他非IO异常:" + e.getMessage();
            status = ResultStatus.default_error;

        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error("httpclien response关闭失败异常", e);
                    errorMessage = "httpclien response关闭失败异常" + e.getMessage();
                }
            }
        }
        if (!status.equals(ResultStatus.success)) {
            errorMessage = "http客户端发起服务调用未成功，原因:" + errorMessage;
        }
        HandlerResponse handlerResponse = new HandlerResponse(result, status, errorMessage);
        return handlerResponse;
    }

    /**
     * 生成每次请求的超时配置
     *
     * @param readTimeout(ms)    读取数据超时时间
     * @param connectTimeout(ms) 建立链接超时时间
     * @return
     */
    private static RequestConfig createRequestTimeoutConfig(int readTimeout, int connectTimeout) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(readTimeout)
                .setConnectTimeout(connectTimeout).build();
        return requestConfig;
    }

    public static void main(String[] args) {
        RequestConfig config = createRequestTimeoutConfig(4000, 500);
        String url = "http://www.baidu.com";
        HttpClientHandler hander = new HttpClientHandler();
        HandlerResponse re = hander.httpGetDemo(url, config, null);
        System.out.println(re);
        String uri = "http://localhost:8181/ods/user/timeout";
        Map map = new HashMap<String, Object>();
        map.put("name", "zhangjinduo");
        re = hander.httpPostDemo(uri, JSONObject.toJSONString(map), config, ContentType.APPLICATION_JSON);
        System.out.println(re);

        map = new HashMap<String, Object>();
        String sql = "select * from yanbo_test";
        uri = "http://10.4.86.12:9200/_xpack/sql?format=json";
        map.put("query", sql);
        re = hander.httpPostDemo(uri, JSONObject.toJSONString(map), config, ContentType.APPLICATION_JSON);
        System.out.println(re);
    }


}
