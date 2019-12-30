/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
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


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creditease.dbus.common.splitters;

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.exception.ValidationException;
import com.creditease.dbus.common.format.DataDBInputSplit;
import com.creditease.dbus.common.format.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


/**
 * Fix bug by Dbus team 20161230
 * Implement DBSplitter over text strings.
 */
public class TextSplitter extends BigDecimalSplitter {

    public static final String ALLOW_TEXT_SPLITTER_PROPERTY = "org.apache.splitter.allow_text_splitter";

    private Logger logger = LoggerFactory.getLogger(getClass());

    private boolean useNCharStrings = false;

    private final int type = Types.VARCHAR;


    private enum SplitterRange {
        ALL, ASSCI, NUMBER, MD5, MD5BIG, UUID, UUIDBIG
    }

    private SplitterRange rangeStyle = SplitterRange.ALL;

    public TextSplitter(String splitRange) {
        if (splitRange.equalsIgnoreCase("assci")) {
            this.rangeStyle = SplitterRange.ASSCI;
        } else if (splitRange.equalsIgnoreCase("number")) {
            this.rangeStyle = SplitterRange.NUMBER;
        } else if (splitRange.equalsIgnoreCase("md5")) {
            this.rangeStyle = SplitterRange.MD5;
        } else if (splitRange.equalsIgnoreCase("md5big")) {
            this.rangeStyle = SplitterRange.MD5BIG;
        } else if (splitRange.equalsIgnoreCase("uuid")) {
            this.rangeStyle = SplitterRange.UUID;
        } else if (splitRange.equalsIgnoreCase("uuidbig")) {
            this.rangeStyle = SplitterRange.UUIDBIG;
        } else {
            this.rangeStyle = SplitterRange.ALL;
        }
        logger.info("TextSplitter----set rangeStyle = " + this.rangeStyle.toString());
    }


    /**
     * This method needs to determine the splits between two user-provided
     * strings.  In the case where the user's strings are 'A' and 'Z', this is
     * not hard; we could create two splits from ['A', 'M') and ['M', 'Z'], 26
     * splits for strings beginning with each letter, etc.
     * <p>
     * If a user has provided us with the strings "Ham" and "Haze", however, we
     * need to create splits that differ in the third letter.
     * <p>
     * The algorithm used is as follows:
     * Since there are 2**16 unicode characters, we interpret characters as
     * digits in base 65536. Given a string 's' containing characters s_0, s_1
     * .. s_n, we interpret the string as the number: 0.s_0 s_1 s_2.. s_n in
     * base 65536. Having mapped the low and high strings into floating-point
     * values, we then use the BigDecimalSplitter to establish the even split
     * points, then map the resulting floating point values back into strings.
     */
    public List<InputSplit> split(long numSplits, ResultSet results,
                                  String colName, DBConfiguration dbConf) throws SQLException, ValidationException {
        if (!dbConf.getAllowTextSplitter()) {
            throw new ValidationException("Generating splits for a textual index column " + "allowed only in case of \"-D"
                    + ALLOW_TEXT_SPLITTER_PROPERTY + "=true\" property " + "passed as a parameter");
        }

        logger.warn("Generating splits for a textual index column.");
        logger.warn("If your database sorts in a case-insensitive order, "
                + "this may result in a partial import or duplicate records.");
        logger.warn("You are strongly encouraged to choose an integral split column.");

        String minString = results.getString(1);
        String maxString = results.getString(2);

        boolean minIsNull = false;

        // If the min value is null, switch it to an empty string instead for
        // purposes of interpolation. Then add [null, null] as a special case
        // split.
        if (null == minString) {
            minString = "";
            minIsNull = true;
        }

        if (null == maxString) {
            // If the max string is null, then the min string has to be null too.
            // Just return a special split for this case.
            List<InputSplit> splits = new ArrayList<InputSplit>();
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
            return splits;
        }

        // Use this as a hint. May need an extra task if the size doesn't
        // divide cleanly.


        // If there is a common prefix between minString and maxString, establish
        // it and pull it out of minString and maxString.
        int maxPrefixLen = Math.min(minString.length(), maxString.length());
        int sharedLen;
        for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
            char c1 = minString.charAt(sharedLen);
            char c2 = maxString.charAt(sharedLen);
            if (c1 != c2) {
                break;
            }
        }

        // The common prefix has length 'sharedLen'. Extract it from both.
        String commonPrefix = minString.substring(0, sharedLen);
        minString = minString.substring(sharedLen);
        maxString = maxString.substring(sharedLen);

        // The common prefix has length 'sharedLen'. Extract it from both.
//    String commonPrefix = minString;
//    try {
//        commonPrefix = ChineseCharHelper.cutString(minString, sharedLen);
//        // 面对中文字符的时候，能截取得更好
//        minString = ChineseCharHelper.cutString(minString,sharedLen,minString.length()-sharedLen);
//        maxString = ChineseCharHelper.cutString(maxString,sharedLen,maxString.length()-sharedLen);
//    }
//    catch (Exception e) {
//      commonPrefix = minString.substring(0, sharedLen);
//      //如果发生异常，就采取这种简单粗暴的方式吧。对程序逻辑没有大影响。
//      minString = minString.substring(sharedLen);
//      maxString = maxString.substring(sharedLen);
//    }

        List<String> splitStrings = split(numSplits, minString, maxString,
                commonPrefix);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // Convert the list of split point strings into an actual set of
        // InputSplits.
        String start = splitStrings.get(0);
        for (int i = 1; i < splitStrings.size(); i++) {
            String end = splitStrings.get(i);
            logger.info("TextSplitter----split_index" + i + " : lower is " + start + "; upper is " + end + ".");
            if (i == splitStrings.size() - 1) {
                // This is the last one; use a closed interval.
                splits.add(new DataDBInputSplit(type, colName, " >= ", start, " <= ", end));
            } else {
                // Normal open-interval case.
                splits.add(new DataDBInputSplit(type, colName, " >= ", start, " < ", end));
            }

            start = end;
        }

        if (minIsNull) {
            // Add the special null split at the end.
            splits.add(new DataDBInputSplit(type, colName, FullPullConstants.QUERY_COND_IS_NULL, null, FullPullConstants.QUERY_COND_IS_NULL, null));
        }

        return splits;
    }

    public List<String> split(long numSplits, String minString,
                              String maxString, String commonPrefix) throws SQLException, ValidationException {

        BigDecimal minVal = stringToBigDecimal(minString);
        BigDecimal maxVal = stringToBigDecimal(maxString);


        if (minVal.compareTo(maxVal) > 0) {
            throw new ValidationException(minVal + " is greater than " + maxVal);
        }

        List<BigDecimal> splitPoints = split(
                new BigDecimal(numSplits), minVal, maxVal);
        List<String> splitStrings = new ArrayList<String>();

        // Convert the BigDecimal splitPoints into their string representations.
        for (BigDecimal bd : splitPoints) {
            splitStrings.add(commonPrefix + bigDecimalToString(bd));
        }


        // Make sure that our user-specified boundaries are the first and last
        // entries in the array.
        if (splitStrings.size() == 0
                || !splitStrings.get(0).equals(commonPrefix + minString)) {
            splitStrings.add(0, commonPrefix + minString);
        }
        if (splitStrings.size() == 1
                || !splitStrings.get(splitStrings.size() - 1).equals(
                commonPrefix + maxString)) {
            splitStrings.add(commonPrefix + maxString);
        }

        return splitStrings;
    }

    private static final BigDecimal ONE_PLACE = new BigDecimal(65536);

    // Maximum number of characters to convert. This is to prevent rounding
    // errors or repeating fractions near the very bottom from getting out of
    // control. Note that this still gives us a huge number of possible splits.
    private static final int MAX_CHARS = 8;

    //changed by Dbus team
    public BigDecimal stringToBigDecimal(String str) {
        if (this.rangeStyle == SplitterRange.ASSCI)
            return stringToBigDecimal_assci(str);
        else if (this.rangeStyle == SplitterRange.NUMBER)
            return stringToBigDecimal_num(str);
        else if (this.rangeStyle == SplitterRange.MD5 || this.rangeStyle == SplitterRange.MD5BIG
                || this.rangeStyle == SplitterRange.UUID || this.rangeStyle == SplitterRange.UUIDBIG)
            // 目前MD5和uuid用同样的处理策略处理
            return stringToBigDecimal_md5(str);
        else
            return stringToBigDecimal_all(str);
    }

    //changed by Dbus team
    public String bigDecimalToString(BigDecimal bd) {
        if (this.rangeStyle == SplitterRange.ASSCI)
            return bigDecimalToString_assci(bd);
        else if (this.rangeStyle == SplitterRange.NUMBER)
            return bigDecimalToString_num(bd);
        else if (this.rangeStyle == SplitterRange.MD5 || this.rangeStyle == SplitterRange.MD5BIG
                || this.rangeStyle == SplitterRange.UUID || this.rangeStyle == SplitterRange.UUIDBIG)
            // 目前MD5和uuid用同样的处理策略处理
            return bigDecimalToString_md5(bd);
        else
            return bigDecimalToString_all(bd);
    }

    /**
     * Return a BigDecimal representation of string 'str' suitable for use in a
     * numerically-sorting order.
     */
    public BigDecimal stringToBigDecimal_all(String str) {
        // Start with 1/65536 to compute the first digit.
        BigDecimal curPlace = ONE_PLACE;
        BigDecimal result = BigDecimal.ZERO;

        int len = Math.min(str.length(), MAX_CHARS);

        for (int i = 0; i < len; i++) {
            int codePoint = str.codePointAt(i);
            result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
            // advance to the next less significant place. e.g., 1/(65536^2) for the
            // second char.
            curPlace = curPlace.multiply(ONE_PLACE);
        }

        return result;
    }

    /**
     * Return the string encoded in a BigDecimal.
     * Repeatedly multiply the input value by 65536; the integer portion after
     * such a multiplication represents a single character in base 65536.
     * Convert that back into a char and create a string out of these until we
     * have no data left.
     */
    public String bigDecimalToString_all(BigDecimal bd) {
        BigDecimal cur = bd.stripTrailingZeros();
        StringBuilder sb = new StringBuilder();

        for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
            cur = cur.multiply(ONE_PLACE);
            int curCodePoint = cur.intValue();
            if (0 == curCodePoint) {
                break;
            }

            cur = cur.subtract(new BigDecimal(curCodePoint));
            //changed by Dbus team
            sb.append(TextSplitter.toChars(curCodePoint));
        }

        return sb.toString();
    }

    //changed by Dbus team
    private static final BigDecimal ASSCI_ONE_PLACE = new BigDecimal(128);

    public BigDecimal stringToBigDecimal_assci(String str) {
        // Start with 1/65536 to compute the first digit.

        //String strUtf8 = new
        BigDecimal curPlace = ASSCI_ONE_PLACE;
        BigDecimal result = BigDecimal.ZERO;

        int len = Math.min(str.length(), MAX_CHARS);

        for (int i = 0; i < len; i++) {
            int ch = str.charAt(i);
            if (ch > 127) {
                ch = 127;
            }
            result = result.add(tryDivide(new BigDecimal(ch), curPlace));
            // advance to the next less significant place. e.g., 1/(128^2) for the
            // second char.
            curPlace = curPlace.multiply(ASSCI_ONE_PLACE);
        }

        return result;
    }

    //changed by Dbus team
    public String bigDecimalToString_assci(BigDecimal bd) {
        BigDecimal cur = bd.stripTrailingZeros();
        StringBuilder sb = new StringBuilder();

        for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
            cur = cur.multiply(ASSCI_ONE_PLACE);
            int ch = cur.intValue();
            if (0 == ch) {
                break;
            }

            cur = cur.subtract(new BigDecimal(ch));

            //替换特殊字符
            if (ch < 33) {
                ch = 33;
            } else if (ch == 127) {
                ch = 126;
            }
            sb.append((char) ch);
        }

        return sb.toString();
    }


    //changed by Dbus team
    private static final int NUM_MAX_CHARS = 8;
    private static final BigDecimal NUM_ONE_PLACE = new BigDecimal(10);
    private static final char[] numCode = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private int charToNumCode(int ch) {
        int code = 0;
        if (ch < (int) '0') {
            // to '0'
            code = 0;
        } else if ((int) '0' <= ch && ch <= (int) '9') {
            code = ch - '0';
        } else {
            // to '9'
            code = 9;
        }
        return code;
    }

    public int numCodeToChar(int code) {
        return (int) md5Code[code];
    }

    public BigDecimal stringToBigDecimal_num(String str) {
        BigDecimal curPlace = NUM_ONE_PLACE;
        BigDecimal result = BigDecimal.ZERO;

        int len = Math.min(str.length(), NUM_MAX_CHARS);

        for (int i = 0; i < len; i++) {
            int ch = str.charAt(i);
            int numCode = charToNumCode(ch);

            result = result.add(tryDivide(new BigDecimal(numCode), curPlace));
            // advance to the next less significant place. e.g., 1/(32^2) for the
            // second char.
            curPlace = curPlace.multiply(NUM_ONE_PLACE);
        }

        return result;
    }

    public String bigDecimalToString_num(BigDecimal bd) {
        BigDecimal cur = bd.stripTrailingZeros();
        StringBuilder sb = new StringBuilder();

        for (int numConverted = 0; numConverted < NUM_MAX_CHARS; numConverted++) {
            cur = cur.multiply(NUM_ONE_PLACE);
            int code = cur.intValue();

            cur = cur.subtract(new BigDecimal(code));
            int ch = numCodeToChar(code);
            sb.append((char) ch);
        }

        return sb.toString();
    }


    private static final int MD5_MAX_CHARS = 8;
    // 0~9, a~f
    private static final BigDecimal MD5_ONE_PLACE = new BigDecimal(16);
    private static final char[] md5Code = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};
    private static final char[] md5CodeBig = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'A', 'B', 'C', 'D', 'E', 'F'};

    private int charToMd5Code(int ch) {
        int code = 0;
        if (ch < (int) '0') {
            // to '0'
            code = 0;
        } else if ((int) '0' <= ch && ch <= (int) '9') {
            code = ch - '0';
        } else if ((int) '9' < ch && ch < (int) 'A') {
            // to '9'
            code = 9;
        } else if ((int) 'A' <= ch && ch <= (int) 'F') {
            code = ch - (int) 'A' + 10;
        } else if ((int) 'F' < ch && ch < (int) 'a') {
            // to 'F'
            code = 15;
        } else if ((int) 'a' <= ch && ch <= (int) 'f') {
            code = ch - (int) 'a' + 10;
        } else {
            // to 'f'
            code = 15;
        }

        return code;
    }

    public int md5CodeToChar(int code) {
        // 目前MD5和uuid用同样的处理策略处理
        switch (this.rangeStyle) {
            case MD5:
            case UUID:
                return (int) md5Code[code];
            case MD5BIG:
            case UUIDBIG:
                return (int) md5CodeBig[code];
            default: // Shouldn't ever hit this case.
                return (int) md5Code[code];
        }


    }

    public BigDecimal stringToBigDecimal_md5(String str) {

        BigDecimal curPlace = MD5_ONE_PLACE;
        BigDecimal result = BigDecimal.ZERO;

        int len = Math.min(str.length(), MD5_MAX_CHARS);

        for (int i = 0; i < len; i++) {
            int ch = str.charAt(i);
            int md5Code = charToMd5Code(ch);

            result = result.add(tryDivide(new BigDecimal(md5Code), curPlace));
            // advance to the next less significant place. e.g., 1/(32^2) for the
            // second char.
            curPlace = curPlace.multiply(MD5_ONE_PLACE);
        }

        return result;
    }

    public String bigDecimalToString_md5(BigDecimal bd) {
        BigDecimal cur = bd.stripTrailingZeros();
        StringBuilder sb = new StringBuilder();

        for (int numConverted = 0; numConverted < MD5_MAX_CHARS; numConverted++) {
            cur = cur.multiply(MD5_ONE_PLACE);
            int code = cur.intValue();

            cur = cur.subtract(new BigDecimal(code));
            int ch = md5CodeToChar(code);
            sb.append((char) ch);
        }

        return sb.toString();
    }

    //changed by Dbus team
    public static char[] toChars(int codePoint) {
        //跳过代理对的保留区
        if (0xD800 <= codePoint && codePoint <= 0xDFFF) {
            codePoint = 0xD3FF;
        }
//	  if ('A' <= codePoint && codePoint <= 'Z') {
//		    codePoint = (int)('A') - 1;
//		  }
        //故意产生合法的emoji字符（大于BMP空间）以产生错误作为测试
        //	  if (codePoint > 0x2000) {
        //		  return new char[] { (char) 0xd83d, (char) 0xde01};
        //	  }
        return new char[]{(char) codePoint};
    }

    public void setUseNCharStrings(boolean use) {
        useNCharStrings = use;
    }

    public boolean isUseNCharStrings() {
        return useNCharStrings;
    }
}
