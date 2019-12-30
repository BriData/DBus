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


package com.creditease.dbus.commons;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;


public class EncrypAES {
    // 密钥：creditease
    private static byte[] keyValue = new byte[]{99, 114, 101, 100, 105, 116, 101, 97, 115, 101};
    private static byte[] iv = new byte[]{ // 算法参数
            -12, 35, -25, 65, 45, -87, 95, -22, -15, 45, 55, -66, 32, 5 - 4, 84, 55};
    private static SecretKey key; // 加密密钥
    private static AlgorithmParameterSpec paramSpec; // 算法参数

    static {
        KeyGenerator kgen;
        try {
            // 为指定算法生成一个密钥生成器对象。
            kgen = KeyGenerator.getInstance("AES");
            // 使用用户提供的随机源初始化此密钥生成器，使其具有确定的密钥长度。
            kgen.init(128, new SecureRandom(keyValue));
            // 使用KeyGenerator生成（对称）密钥。
            key = kgen.generateKey();
            // 使用iv中的字节作为IV来构造一个 算法参数。
            paramSpec = new IvParameterSpec(iv);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 加密，使用指定数据源生成密钥，使用用户数据作为算法参数进行AES加密
     *
     * @param msg 加密的数据
     * @return
     */
    public static String encrypt(String msg) {
        try {
            // 生成一个实现指定转换的 Cipher 对象
            Cipher ecipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            // 用密钥和一组算法参数初始化此 cipher
            ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
            // 加密并转换成16进制字符串
            return asHex(ecipher.doFinal(msg.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 解密，对生成的16进制的字符串进行解密
     *
     * @param value 解密的数据
     * @return
     */
    public static String decrypt(String value) {
        try {
            Cipher ecipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            ecipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
            return new String(ecipher.doFinal(asBin(value)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将字节数组转换成16进制字符串
     *
     * @param buf
     * @return
     */
    private static String asHex(byte buf[]) {
        StringBuffer strbuf = new StringBuffer(buf.length * 2);
        int i;
        for (i = 0; i < buf.length; i++) {
            if (((int) buf[i] & 0xff) < 0x10)// 小于十前面补零
                strbuf.append("0");
            strbuf.append(Long.toString((int) buf[i] & 0xff, 16));
        }
        return strbuf.toString();
    }

    /**
     * 将16进制字符串转换成字节数组
     *
     * @param src
     * @return
     */
    private static byte[] asBin(String src) {
        if (src.length() < 1)
            return null;
        byte[] encrypted = new byte[src.length() / 2];
        for (int i = 0; i < src.length() / 2; i++) {
            int high = Integer.parseInt(src.substring(i * 2, i * 2 + 1), 16);// 取高位字节
            int low = Integer.parseInt(src.substring(i * 2 + 1, i * 2 + 2), 16);// 取低位字节
            encrypted[i] = (byte) (high * 16 + low);
        }
        return encrypted;
    }

    public static void main(String[] args) {
        long n = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            encrypt("createease");

        }
        System.out.println(System.currentTimeMillis() - n);
    }
}
