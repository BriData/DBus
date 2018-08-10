/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import com.creditease.dbus.router.bean.Ack;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created by Administrator on 2018/6/4.
 */
public class TestQueue implements Watcher {

    private Map<Long, Ack> queue = new TreeMap<>(new TestQueue.AckComparator());

    private static final int SESSION_TIME   = 2000;
    protected static ZooKeeper zooKeeper;
    protected static CountDownLatch countDownLatch=new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException, IOException {

        /*TestQueue tq = new TestQueue();
        AckVo ackVo_0 = new AckVo();
        tq.queue.put(0l, ackVo_0);
        AckVo ackVo_100 = new AckVo();
        tq.queue.put(100l, ackVo_100);
        AckVo ackVo_500 = new AckVo();
        tq.queue.put(500l, ackVo_500);
        AckVo ackVo_30 = new AckVo();
        tq.queue.put(30l, ackVo_30);
        AckVo ackVo_50 = new AckVo();
        tq.queue.put(50l, ackVo_50);
        AckVo ackVo_10 = new AckVo();
        tq.queue.put(10l, ackVo_10);
        AckVo ackVo_120 = new AckVo();
        tq.queue.put(120l, ackVo_120);
        AckVo ackVo_110 = new AckVo();
        tq.queue.put(110l, ackVo_110);
        AckVo ackVo_330 = new AckVo();
        tq.queue.put(330l, ackVo_330);
        AckVo ackVo_300 = new AckVo();
        tq.queue.put(300l, ackVo_300);
        AckVo ackVo_600 = new AckVo();
        tq.queue.put(600l, ackVo_600);

        for (Long key : tq.queue.keySet()) {
            System.out.println(key);
        }*/

        //System.setSecurityManager(new SecurityManager());
        //System.setProperty("java.security.auth.login.config", "E:\\GitWorkSpace\\bridata\\dbus\\dbus-main\\dbus-router\\src\\test\\java\\jaas.conf");
        /*TestQueue testQueue = new TestQueue();
        zooKeeper = new ZooKeeper("vdbus-7:2181", SESSION_TIME, testQueue);
        countDownLatch.await();*/

        /*Map<String, Integer> map = new HashMap<>();
        map.remove("aaa");
        System.out.println(map.get("aaa") == 1);*/

        /*String status = "stopped";
        String strData = get("http://vdbus-4:8080/api/v1/topology/summary");
        System.out.println("str data: " + strData);
        JSONObject data = JSON.parseObject(strData);
        JSONArray topologies = data.getJSONArray("topologies");
        for (JSONObject topology : topologies.toJavaList(JSONObject.class)) {
            String name = topology.getString("name");
            String wk_status = topology.getString("status");
            if (StringUtils.equals(name, "tr-router") && StringUtils.equals(wk_status, "ACTIVE")) {
                status = "running";
            }
        }
        System.out.println(status);*/

        // post("http://vdbus-6:3000/api/dashboards/db");
        //System.out.println(get("http://vdbus-6:3000/api/dashboards/db/api_test"));



    }

    public static void post(String api) {
        URL url;
        BufferedWriter bw = null;
        BufferedReader br = null;
        StringBuilder responseBuilder = null;
        try {
            url = new URL(api);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setConnectTimeout(1000 * 5);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", "Bearer eyJrIjoiaVFkNWRpODQ5VkNnRFlZYjFLbEQxeE9JSnJCS2k2aTgiLCJuIjoiYXBpX3Rlc3Rfa2V5cyIsImlkIjoxfQ==");

            String data = "{\"dashboard\": {\"id\": null,\"uid\": null,\"title\": \"api_test\",\"tags\": [ \"templated\" ],\"timezone\": \"browser\",\"schemaVersion\": 16,\"version\": 0, \"rows\":[{\"aliasColors\":{},\"bars\":false,\"datasource\":\"inDB\",\"editable\":true,\"error\":false,\"fill\":1,\"grid\":{\"threshold1\":null,\"threshold1Color\":\"rgba(216, 200, 27, 0.27)\",\"threshold2\":null,\"threshold2Color\":\"rgba(234, 112, 112, 0.22)\"},\"id\":1,\"interval\":\">600d\",\"isNew\":true,\"legend\":{\"avg\":true,\"current\":false,\"max\":true,\"min\":true,\"show\":true,\"total\":true,\"values\":true},\"lines\":true,\"linewidth\":2,\"links\":[],\"nullPointMode\":\"connected\",\"percentage\":false,\"pointradius\":5,\"points\":false,\"renderer\":\"flot\",\"seriesOverrides\":[],\"span\":12,\"stack\":false,\"steppedLine\":false,\"targets\":[{\"alias\":\"分发器计数\",\"dsType\":\"influxdb\",\"groupBy\":[{\"params\":[\"1m\"],\"type\":\"time\"}],\"hide\":false,\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"refId\":\"A\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"},{\"params\":[],\"type\":\"sum\"}]],\"tags\":[{\"key\":\"schema\",\"operator\":\"=~\",\"value\":\"/^$schema$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"DISPATCH_TYPE\"}]},{\"alias\":\"增量计数\",\"dsType\":\"influxdb\",\"groupBy\":[{\"params\":[\"1m\"],\"type\":\"time\"}],\"hide\":false,\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"refId\":\"C\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"},{\"params\":[],\"type\":\"sum\"}]],\"tags\":[{\"key\":\"schema\",\"operator\":\"=~\",\"value\":\"/^$schema$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"APPENDER_TYPE\"}]},{\"alias\":\"日志计数\",\"dsType\":\"influxdb\",\"groupBy\":[{\"params\":[\"1m\"],\"type\":\"time\"}],\"hide\":false,\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"refId\":\"B\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"},{\"params\":[],\"type\":\"sum\"}]],\"tags\":[{\"key\":\"schema\",\"operator\":\"=~\",\"value\":\"/^$schema$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"log-plain\"}]}],\"timeFrom\":null,\"timeShift\":null,\"title\":\"schema统计计数\",\"tooltip\":{\"msResolution\":true,\"shared\":true,\"sort\":0,\"value_type\":\"cumulative\"},\"type\":\"graph\",\"xaxis\":{\"show\":true},\"yaxes\":[{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true},{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true}]}]},\"folderId\": 0,\"overwrite\": false}";
            bw = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
            bw.write(data);
            bw.flush();

            int responseCode = conn.getResponseCode();
            System.out.println("response code: " + responseCode);

            br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            responseBuilder = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                responseBuilder.append(line).append("\n");
            }
        } catch (Exception e) {

        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
        System.out.println(responseBuilder.toString());
    }

    public static String get(String serverUrl) {
        StringBuilder responseBuilder = null;
        BufferedReader reader = null;

        URL url;
        try {
            url = new URL(serverUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setDoOutput(true);
            conn.setConnectTimeout(1000 * 5);

            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            responseBuilder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line).append("\n");
            }
        } catch (IOException e) {
        } finally {

            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }

        }

        return responseBuilder.toString();
    }

    public void commit(Ack ack) {

    }

    public void rollback(Ack ack) {

    }

    private class AckComparator implements Comparator<Long> {
        @Override
        public int compare(Long o1, Long o2) {
            return (int) (o1 - o2);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
        if(event.getState()== Event.KeeperState.SyncConnected){
            countDownLatch.countDown();
        }
    }

    public void close() throws InterruptedException{
        zooKeeper.close();
    }

}
