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


package com.creditease.dbus.utils;

import io.jsonwebtoken.*;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyf on 2018/3/9.
 */
public class JWTUtils {
    public static String buildToken(String key, long expirationMinutes, Map<String, Object> claims) {

        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;

        long nowMillis = System.currentTimeMillis();
        Date now = new Date(nowMillis);

        byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(key);
        Key signingKey = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());

        JwtBuilder builder = Jwts.builder().setIssuedAt(now)
                .addClaims(claims)
                .signWith(signatureAlgorithm, signingKey);
        builder.setExpiration(new Date(nowMillis + expirationMinutes * 60 * 1000));

        return builder.compact();
    }

    public static Jws<Claims> parseToken(String token, String key) {
        return Jwts.parser()
                .setSigningKey(DatatypeConverter.parseBase64Binary(key))
                .parseClaimsJws(token);
    }

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("userId", "000");
        String SIGNING_KEY = "sign_key";
        String token = JWTUtils.buildToken(SIGNING_KEY, 100000, map);
        System.out.println(token);

        Jws<Claims> jws = parseToken(token, SIGNING_KEY);
        System.out.println(jws.getBody().get("userId"));
    }
}
