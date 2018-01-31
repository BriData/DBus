/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.heartbeat.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.heartbeat.vo.GlobalControlEmailVo;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.List;

public class HTMLUtil {

    public static String globalControlEmailJsonVo2HTML(JSONObject json) {
        JSONObject payload = json.getJSONObject("payload");
        JSONObject beforeMap = payload.getJSONObject("before");
        JSONObject afterMap = payload.getJSONObject("after");

        final String HTML_LINE_SEPARATOR = "<br/>";
//        final String HTML_INDENT = "&nbsp;&nbsp;";
        StringBuilder output = new StringBuilder();
        output.append("您好，数据库表结构发生变更！！！"+HTML_LINE_SEPARATOR);
        output.append("时间：" + json.getString("timestamp") + HTML_LINE_SEPARATOR);
        output.append("表名：" + StringUtils.join(new Object[] {payload.get("datasource"),payload.get("schema"),payload.get("table")}, ".") + HTML_LINE_SEPARATOR);
        output.append("新版本号：" + payload.getString("version") + HTML_LINE_SEPARATOR);
        output.append("变更前后对比:" + HTML_LINE_SEPARATOR);
        output.append(compareListHTML(beforeMap.getJSONArray("columns"),afterMap.getJSONArray("columns")));
        return output.toString();
    }

    private static String transformToHtml(String[][] table, boolean[] needHighlight, String[] header) {
        StringBuffer result = new StringBuffer();
        result.append("<table bgcolor=\"#c1c1c1\">");

        result.append("<tr bgcolor=\"#ffffff\">");
        result.append("<th colspan=\"" + header.length + "\">");
        result.append("变更前");
        result.append("</th>");
        result.append("<th colspan=\"" + header.length + "\">");
        result.append("变更后");
        result.append("</th>");
        result.append("</tr>");


        result.append("<tr bgcolor=\"#ffffff\">");
        for (int c = 0; c < 2; c++) {
            for (int i = 0; i < header.length; i++) {
                result.append("<th>");
                result.append(header[i]);
                result.append("</th>");
            }
        }
        result.append("</tr>");


        for(int i=0;i<table.length;i++) {
            result.append("<tr bgcolor=\"#ffffff\">");
            for(int j=0;j<header.length*2;j++) {
                if(needHighlight[i]) result.append("<td bgcolor=\"#FFC0CB\">");
                else result.append("<td>");
                if (table[i][j] != null) result.append(table[i][j]);
                result.append("</td>");
            }
            result.append("</tr>");
        }


        result.append("</table>");

        return result.toString();
    }

    private static String compareListHTML(JSONArray ori, JSONArray now) {
        if (ori == null) ori = new JSONArray();
        if (now == null) now = new JSONArray();
        Collections.sort(ori, (o1, o2) -> {
            JSONObject jo1 = (JSONObject) o1;
            JSONObject jo2 = (JSONObject) o2;
            return jo1.getString("columnName").compareTo(jo2.getString("columnName"));
        });
        Collections.sort(now, (o1, o2) -> {
            JSONObject jo1 = (JSONObject) o1;
            JSONObject jo2 = (JSONObject) o2;
            return jo1.getString("columnName").compareTo(jo2.getString("columnName"));
        });
        final int ORI_LENGTH_SUB_1 = 1;
        final int NOW_LENGTH_SUB_1 = 2;
        final int ORI_AND_NOW_LENGTH_SUB_1 = 3;
        String[] header = new String[]{"columnName", "dataType", "dataLength", "dataScale", "comments"};
        int[][] dp = new int[ori.size() + 1][now.size() + 1];
        int[][] path = new int[ori.size() + 1][now.size() + 1];
        for (int i = 1; i <= ori.size(); i++) path[i][0] = ORI_LENGTH_SUB_1;
        for (int j = 1; j <= now.size(); j++) path[0][j] = NOW_LENGTH_SUB_1;
        for (int i = 1; i <= ori.size(); i++) {
            for (int j = 1; j <= now.size(); j++) {
                if (StringUtils.equals(ori.getJSONObject(i - 1).getString(header[0]),
                        now.getJSONObject(j - 1).getString(header[0]))) {
                    path[i][j] = ORI_AND_NOW_LENGTH_SUB_1;
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    if (dp[i - 1][j] > dp[i][j - 1]) {
                        path[i][j] = ORI_LENGTH_SUB_1;
                        dp[i][j] = dp[i - 1][j];
                    } else {
                        path[i][j] = NOW_LENGTH_SUB_1;
                        dp[i][j] = dp[i][j - 1];
                    }
                }
            }
        }
        int resultLength = ori.size() + now.size() - dp[ori.size()][now.size()];
        String[][] result = new String[resultLength][header.length * 2];
        boolean[] needHighlight = new boolean[resultLength];
        for (int i = resultLength - 1, posOri = ori.size(), posNow = now.size(); i >= 0; i--) {
            if (path[posOri][posNow] == ORI_LENGTH_SUB_1) {
                setResult(result[i], 0, header, ori.getJSONObject(posOri - 1));
                needHighlight[i] = true;
                posOri--;
            } else if (path[posOri][posNow] == NOW_LENGTH_SUB_1) {
                setResult(result[i], header.length, header, now.getJSONObject(posNow - 1));
                needHighlight[i] = true;
                posNow--;
            } else {
                setResult(result[i], 0, header, ori.getJSONObject(posOri - 1));
                setResult(result[i], header.length, header, now.getJSONObject(posNow - 1));
                needHighlight[i] = !fieldEqual(ori.getJSONObject(posOri - 1), now.getJSONObject(posNow - 1), header);
                posOri--;
                posNow--;
            }
        }
        return transformToHtml(result, needHighlight, header);
    }

    private static void setResult(String[] resultRow, int start, String[] header, JSONObject json) {
        for (int i = 0; i < header.length; i++) {
            resultRow[start + i] = json.getString(header[i]);
        }
    }

    private static boolean fieldEqual(JSONObject jsonObject1, JSONObject jsonObject2, String[] header) {
        for (String s : header) {
            if (jsonObject1.get(s) == null && jsonObject2.get(s) == null) continue;
            if (jsonObject1.get(s) == null || jsonObject2.get(s) == null) return false;
            if (!jsonObject1.get(s).equals(jsonObject2.get(s))) return false;
        }
        return true;
    }
}
