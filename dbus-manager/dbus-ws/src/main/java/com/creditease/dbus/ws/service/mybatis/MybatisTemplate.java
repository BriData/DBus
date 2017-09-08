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

package com.creditease.dbus.ws.service.mybatis;

import com.creditease.dbus.ws.SessionUtil;
import org.apache.ibatis.session.SqlSession;

/**
 * 通过Mybatis执行数据库操作的模板
 * Created by Shrimp on 16/9/2.
 */
public class MybatisTemplate {
    public interface MybatisExecutor<T> {
        T execute(SqlSession session, Object... args);
    }

    public static MybatisTemplate template() {
        return new MybatisTemplate();
    }

    /**
     * 执行查询返回结果
     * @param args 执行参数
     * @param executor 执行器
     * @param <T> 返回指定类型的结果
     * @return 返回值
     */
    public <T> T query(Object[] args, MybatisExecutor<T> executor) {
        SqlSession sqlSession = getSession();
        try {
            return executor.execute(sqlSession, args);
        } finally {
            sqlSession.close();
        }
    }

    /**
     * 执行查询返回结果
     * @param executor 执行器
     * @param <T> 返回指定类型的结果
     * @return 返回值
     */
    public <T> T query(MybatisExecutor<T> executor) {
        SqlSession sqlSession = getSession();
        try {
            return executor.execute(sqlSession);
        } finally {
            sqlSession.close();
        }
    }

    /**
     * 执行数据变更,成功提交失败回滚
     * @param executor 执行器
     * @param <T> 返回指定类型的结果
     * @return 返回值
     */
    public <T> T update(MybatisExecutor<T> executor) {
        SqlSession sqlSession = getSession();
        try {
            T t = executor.execute(sqlSession);
            sqlSession.commit();
            return t;
        } catch (Exception e) {
            sqlSession.rollback();
            throw new MybatisExecutorException(e);
        } finally {
            sqlSession.close();
        }
    }

    private SqlSession getSession() {
        return SessionUtil.openSession();
    }
}
