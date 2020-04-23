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


package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.mapper.EncodePluginsMapper;
import com.creditease.dbus.domain.mapper.ProjectMapper;
import com.creditease.dbus.domain.mapper.TopologyJarMapper;
import com.creditease.dbus.domain.model.EncodePlugins;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.TopologyJar;
import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.PluginManager;
import com.creditease.dbus.encoders.PluginScanner;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mal on 2018/4/19.
 */
@Service
public class JarManagerService {

    private static Pattern pattern = Pattern.compile("([\\\\/]{1}[\\d\\w\\-_\\.]+){3}[\\\\/]{1}[\\d\\w\\-_\\.]+\\.jar");
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private IZkService zkService;
    @Autowired
    private EncodePluginsMapper encodePluginsMapper;
    @Autowired
    private ProjectMapper projectMapper;
    @Autowired
    private TopologyJarMapper topologyJarMapper;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;

    public int uploads(String category, String version, String type, MultipartFile jarFile) {
        int success = 1;
        File file = null;
        try {
            file = new File(SystemUtils.getUserDir(), System.currentTimeMillis() + ".jar");
            jarFile.transferTo(file);

            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
            String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
            String baseDir = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_JARS_PATH);

            long time = System.currentTimeMillis();
            String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
            String savePath = StringUtils.join(new String[]{baseDir, version, type, minorVersion}, "/");

            // 0:ssh port, 1:user name, 2:nimbus host, 3:path
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} mkdir -pv {3}", port, user, stormNimbusHost, savePath);
            logger.info("mdkir command: {}", cmd);
            int retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("mkdir success.");
                // 0:ssh port, 1:source path, 2:user name, 3:nimbus host, 4:dest path
                cmd = MessageFormat.format("scp -P {0} {1} {2}@{3}:{4}",
                        port, file.getPath(), user, stormNimbusHost, savePath + "/" + jarFile.getOriginalFilename());
                logger.info("scp command: {}", cmd);
                retVal = execCmd(cmd, null);
                if (retVal == 0) {
                    logger.info("scp success.");
                }
            }
            TopologyJar topologyJar = new TopologyJar();
            topologyJar.setName(jarFile.getOriginalFilename());
            topologyJar.setCategory(category);
            topologyJar.setVersion(version);
            topologyJar.setType(type);
            topologyJar.setMinorVersion(minorVersion);
            topologyJar.setPath(savePath + "/" + jarFile.getOriginalFilename());
            topologyJar.setCreateTime(new Date());
            topologyJar.setUpdateTime(new Date());
            topologyJarMapper.insert(topologyJar);

        } catch (Exception e) {
            success = 0;
        } finally {
            if (file != null) file.delete();
        }
        return success;
    }

    public int batchDelete(List<Integer> ids) {
        int ret = 0;
        for (Integer id : ids) {
            ret += delete(id);
        }
        return ret;
    }

    public int delete(Integer id) {
        TopologyJar topologyJar = topologyJarMapper.selectByPrimaryKey(id);
        try {
            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
            String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

            String path = topologyJar.getPath();
            String parentPath = path.substring(0, path.lastIndexOf("/"));
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} rm -rf {3}", port, user, stormNimbusHost, parentPath);
            logger.info("mdkir command: {}", cmd);
            int retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("rm success.");
            }
            return topologyJarMapper.deleteByPrimaryKey(topologyJar.getId());
        } catch (Exception e) {
            return 0;
        }
    }

    public List<String> queryType(String category, String version) {
        ArrayList<String> list = new ArrayList<>();
        if (StringUtils.equals(category, "router")) {
            list.add(KeeperConstants.TYPE_ROUTER);
        } else if (StringUtils.equals(category, "sinker")) {
            list.add(KeeperConstants.TYPE_SINKER);
        } else {
            list.add(KeeperConstants.TYPE_MYSQL_EXTRACTOR);
            list.add(KeeperConstants.TYPE_DISPATCHER_APPENDER);
            list.add(KeeperConstants.TYPE_LOG_PROCESSOR);
            list.add(KeeperConstants.TYPE_SPLITTER_PULLER);
        }
        return list;
    }

    public List<String> queryVersion(String category) {
        HashMap<Object, Object> param = new HashMap<>();
        param.put("category", category);
        Set<String> collect = topologyJarMapper.search(param).stream().map(TopologyJar::getVersion).collect(Collectors.toSet());
        ArrayList<String> list = new ArrayList<>();
        list.addAll(collect);
        if (list.isEmpty()) {
            list.add(KeeperConstants.RELEASE_VERSION);
        }
        return list;
    }

    public List<TopologyJar> queryJarInfos(String category, String version, String type) {
        category = category == null ? "normal" : category;
        HashMap<Object, Object> param = new HashMap<>();
        param.put("category", category);
        param.put("version", version);
        param.put("type", type);
        return topologyJarMapper.search(param);
    }

    public Integer syncJarInfos() throws Exception {
        Map<String, Map<String, String>> jarInfos = getJarInfos();
        List<TopologyJar> topologyJars = topologyJarMapper.selectAll();
        //去除数据库无效的jar包记录
        Iterator<TopologyJar> iterator = topologyJars.iterator();
        while (iterator.hasNext()) {
            TopologyJar jar = iterator.next();
            if (null == jarInfos.get(jar.getPath())) {
                topologyJarMapper.deleteByPrimaryKey(jar.getId());
                iterator.remove();
                logger.info("delete topology jar {}", JSON.toJSONString(jar));
            }
        }
        HashMap<String, TopologyJar> topologyJarsMap = new HashMap<>();
        for (TopologyJar topologyJar : topologyJars) {
            topologyJarsMap.put(topologyJar.getPath(), topologyJar);
        }
        jarInfos.entrySet().forEach(entry -> {
            TopologyJar jar = topologyJarsMap.get(entry.getKey());
            Map<String, String> value = entry.getValue();
            if (jar == null) {
                TopologyJar topologyJarNew = new TopologyJar();
                topologyJarNew.setCategory(getCategory(value.get("type")));
                topologyJarNew.setName(value.get("name"));
                topologyJarNew.setType(value.get("type"));
                topologyJarNew.setVersion(value.get("version"));
                topologyJarNew.setMinorVersion(value.get("minorVersion"));
                topologyJarNew.setPath(value.get("path"));
                topologyJarNew.setCreateTime(new Date());
                topologyJarNew.setUpdateTime(new Date());
                topologyJarMapper.insert(topologyJarNew);
                logger.info("new topology jar {}", JSON.toJSONString(topologyJarNew));
            }
        });
        return 0;
    }

    private String getCategory(String type) {
        if ("router".equals(type)) {
            return "router";
        } else if ("sinker".equals(type)) {
            return "sinker";
        } else {
            return "normal";
        }
    }

    private Map<String, Map<String, String>> getJarInfos() throws Exception {
        Map<String, Map<String, String>> jarInfos = new HashMap<>();
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String baseDir = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_JARS_PATH);
        String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -type f -a -name \"*.jar\"", user, stormNimbusHost, port, baseDir);
        logger.info("cmd command: {}", cmd);

        List<String> jars = new ArrayList<>();
        int retVal = execCmd(cmd, jars);
        if (retVal == 0) {
            logger.info("obtain jars info success.");
            for (String jarPath : jars) {
                Map<String, String> record = new HashMap<>();
                Matcher m = pattern.matcher(jarPath);
                if (m.find()) {
                    String[] arrs = m.group().split("[\\\\/]");
                    record.put("version", arrs[1]);
                    record.put("type", arrs[2]);
                    record.put("minorVersion", arrs[3]);
                    record.put("name", arrs[4]);
                    record.put("path", jarPath);
                    jarInfos.put(jarPath, record);
                }
            }
        }
        return jarInfos;
    }

    public Integer updateJar(TopologyJar jar) {
        return topologyJarMapper.updateByPrimaryKey(jar);
    }

    private String obtainBaseDir(String proName) throws Exception {
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        return props.getProperty(proName);
    }

    public int uploadsEncodePlugin(String name, Integer projectId, MultipartFile jarFile) {
        int retVal = 0;
        File file = null;
        try {
            file = new File(SystemUtils.getUserDir(), System.currentTimeMillis() + ".jar");
            jarFile.transferTo(file);
            String destPath = uploadsEncodePlugin(file, jarFile.getOriginalFilename(), projectId);
            getAndSaveEncodePlugins(name, file, projectId, destPath);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (file != null) file.delete();
        }
        return retVal;
    }

    private String uploadsEncodePlugin(File file, String fileName, Integer projectId) throws Exception {
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String baseDir = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_ENCODE_JARS_PATH);
        long time = System.currentTimeMillis();
        String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
        String savePath = StringUtils.join(new String[]{baseDir, projectId.toString(), minorVersion}, "/");

        String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
        String destPath = savePath + "/" + fileName;
        for (String host : getStormSupervisors()) {
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} mkdir -pv {3}", port, user, host, savePath);
            logger.info("mdkir command: {}", cmd);
            int result = execCmd(cmd, null);
            if (result == 0) {
                logger.info("mkdir success.");
                cmd = MessageFormat.format("scp -P {0} {1} {2}@{3}:{4}", port, file.getPath(), user, host, destPath);
                logger.info("scp command: {}", cmd);
                execCmd(cmd, null);
            }
        }
        logger.info("scp encode plugins success.");
        return destPath;
    }

    private int getAndSaveEncodePlugins(String name, File file, Integer projectId, String destPath) throws Exception {
        //解析jar包 获取被注解Encoder标识类的type合集
        String filePath = file.getPath();
        String types = getPluginsEncoderTypes(filePath);
        if (StringUtils.isBlank(types)) {
            return MessageCode.ENCODE_PLUGIN_JAR_IS_WRONG;
        }
        types = types.substring(0, types.length() - 1);
        EncodePlugins encodePlugin = new EncodePlugins();
        encodePlugin.setName(name);
        encodePlugin.setPath(destPath);
        encodePlugin.setProjectId(projectId);
        encodePlugin.setStatus(KeeperConstants.ACTIVE);
        encodePlugin.setEncoders(types);
        encodePluginsMapper.insert(encodePlugin);
        return 0;
    }

    private ArrayList<String> getStormSupervisors() {
        ArrayList<String> list = new ArrayList<>();
        try {
            JSONArray supervisors = JSONObject.parseObject(stormTopoHelper.supervisorSummary()).getJSONArray("supervisors");
            for (int i = 0; i < supervisors.size(); i++) {
                JSONObject supervisor = (JSONObject) supervisors.get(i);
                list.add(supervisor.getString("host"));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return list;
    }

    public int uploadsKeytab(Integer projectId, String principal, MultipartFile jarFile) throws Exception {
        String baseDir = obtainBaseDir(KeeperConstants.GLOBAL_CONF_KEY_KEYTAB_FILE_PATH);
        long time = System.currentTimeMillis();
        String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
        String savePath = StringUtils.join(new String[]{projectId.toString(), minorVersion}, "/");
        File saveDir = new File(baseDir, savePath);
        if (!saveDir.exists()) saveDir.mkdirs();
        File file = new File(saveDir, jarFile.getOriginalFilename());
        jarFile.transferTo(file);

        Project project = projectMapper.selectByPrimaryKey(projectId);
        if (StringUtils.isNotBlank(project.getKeytabPath())) {
            File oldFile = new File(project.getKeytabPath());
            File parentFile = oldFile.getParentFile();
            if (oldFile.exists()) oldFile.delete();
            if (parentFile != null && parentFile.exists()) parentFile.delete();
        }
        project.setKeytabPath(file.getPath());
        project.setPrincipal(principal);
        projectMapper.updateByPrimaryKey(project);
        return 0;
    }

    private String getPluginsEncoderTypes(String jarPath) throws Exception {
        URL jar = new URL("file", null, jarPath);
        PluginScanner scanner = new ReflectionsPluginScanner(ConfigurationBuilder.build(jar));
        Iterable<String> encoders = scanner.scan(Encoder.class);
        StringBuilder sb = new StringBuilder();
        if (encoders.iterator().hasNext()) {
            PluginsClassloader classloader = new PluginsClassloader(PluginManager.class.getClassLoader());
            classloader.addJar(jar);
            for (String encoderClass : encoders) {
                Class<?> clazz = classloader.loadClass(encoderClass);
                Encoder e = clazz.getDeclaredAnnotation(Encoder.class);
                sb.append(e.type()).append(",");
            }
        }
        return sb.toString();
    }

    public PageInfo<EncodePlugins> searchEncodePlugin(int pageNum, int pageSize) {
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(encodePluginsMapper.selectAll());
    }

    public int deleteEncodePlugin(Integer id) {
        int num = encodePluginsMapper.numberOfPluginUse(id);
        //说明插件还在使用不能删除
        if (num != 0) {
            return num;
        }
        EncodePlugins encodePlugins = encodePluginsMapper.selectByPrimaryKey(id);
        try {
            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

            String path = encodePlugins.getPath();
            String parentPath = path.substring(0, path.lastIndexOf("/"));
            for (String host : getStormSupervisors()) {
                String cmd = MessageFormat.format("ssh -p {0} {1}@{2} rm -rf {3}", port, user, host, parentPath);
                logger.info("mdkir command: {}", cmd);
                int retVal = execCmd(cmd, null);
                if (retVal == 0) {
                    logger.info("rm success.");
                }
            }
            encodePluginsMapper.deleteByPrimaryKey(id);
        } catch (Exception e) {
            return 0;
        }
        return 0;
    }

    public int initDbusJars() {
        try {
            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String host = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
            String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
            String homePath = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_JARS_PATH);

            uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_NORMAL, KeeperConstants.TYPE_LOG_PROCESSOR, KeeperConstants.JAR_NAME_LOG_PROCESSOR);
            logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_LOG_PROCESSOR);

            //uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_NORMAL, KeeperConstants.TYPE_MYSQL_EXTRACTOR, KeeperConstants.JAR_NAME_MYSQL_EXTRACTOR);
            //logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_MYSQL_EXTRACTOR);

            uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_NORMAL, KeeperConstants.TYPE_DISPATCHER_APPENDER, KeeperConstants.JAR_NAME_DISPATCHER_APPENDER);
            logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_DISPATCHER_APPENDER);

            uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_NORMAL, KeeperConstants.TYPE_SPLITTER_PULLER, KeeperConstants.JAR_NAME_SPLITTER_PULLER);
            logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_SPLITTER_PULLER);

            uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_ROUTER, KeeperConstants.TYPE_ROUTER, KeeperConstants.JAR_NAME_ROUTER);
            logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_ROUTER);

            uploadStormJars(user, host, port, homePath, KeeperConstants.CATEGORY_SINKER, KeeperConstants.TYPE_SINKER, KeeperConstants.JAR_NAME_SINKER);
            logger.info("上传jar:{}包成功.", KeeperConstants.JAR_NAME_SINKER);

            syncJarInfos();
            initEncodePlugins();
            return 0;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return MessageCode.EXCEPTION;
        }
    }

    private void initEncodePlugins() throws Exception {
        File file = new File(SystemUtils.getUserDir() + "/../extlib/" + KeeperConstants.JAR_NAME_ENCODER_PLUGINS);
        String name = KeeperConstants.JAR_NAME_ENCODER_PLUGINS.substring(0, KeeperConstants.JAR_NAME_ENCODER_PLUGINS.lastIndexOf("."));
        String destPath = uploadsEncodePlugin(file, name, 0);
        EncodePlugins encodePlugin = new EncodePlugins();
        encodePlugin.setName(name);
        encodePlugin.setPath(destPath);
        encodePlugin.setProjectId(0);
        encodePlugin.setStatus(KeeperConstants.ACTIVE);
        encodePlugin.setEncoders("md5,default-value,murmur3,regex,replace");
        encodePluginsMapper.insert(encodePlugin);
    }

    private void uploadStormJars(String user, String host, String port, String homePath, String category, String type, String jarName) {
        long time = System.currentTimeMillis();
        String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
        String savePath = StringUtils.join(new String[]{homePath, KeeperConstants.RELEASE_VERSION, type, minorVersion}, "/");
        String cmd = MessageFormat.format("ssh -p {0} {1}@{2} mkdir -pv {3}", port, user, host, savePath);
        logger.info("mdkir command: {}", cmd);
        int retVal = execCmd(cmd, null);
        if (retVal == 0) {
            logger.info("mkdir success.");
            File file = new File(SystemUtils.getUserDir() + "/../extlib/" + jarName);
            cmd = MessageFormat.format("scp -P {0} {1} {2}@{3}:{4}", port, file.getPath(), user, host, savePath + "/" + jarName);
            logger.info("scp command: {}", cmd);
            retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("scp success.");
            }
        }
        TopologyJar topologyJar = new TopologyJar();
        topologyJar.setName(jarName);
        topologyJar.setCategory(category);
        topologyJar.setVersion(KeeperConstants.RELEASE_VERSION);
        topologyJar.setType(type);
        topologyJar.setMinorVersion(minorVersion);
        topologyJar.setPath(savePath + "/" + jarName);
        topologyJar.setCreateTime(new Date());
        topologyJar.setUpdateTime(new Date());
        topologyJarMapper.insert(topologyJar);
    }

    class ReflectionsPluginScanner extends Reflections implements PluginScanner {
        public ReflectionsPluginScanner(Configuration configuration) {
            super(configuration);
        }

        @Override
        public Iterable<String> scan(final Class<? extends Annotation> annotation) {
            Iterable<String> annotated = store.get(TypeAnnotationsScanner.class.getSimpleName(), annotation.getName());
            return getAllAnnotated(annotated, annotation.isAnnotationPresent(Inherited.class), false);
        }
    }

    class PluginsClassloader extends URLClassLoader {
        public PluginsClassloader(ClassLoader parent) {
            super(new URL[0], parent);
        }

        public void addJar(URL jar) throws Exception {
            this.addURL(jar);
        }
    }

    private int execCmd(String cmd, List<String> lines) {
        int exitValue = -1;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), lines));
            // Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), lines));
            outThread.start();
            // errThread.start();
            exitValue = process.waitFor();
            if (lines != null)
                logger.info("result: " + JSON.toJSONString(lines));
            if (exitValue != 0) process.destroyForcibly();
        } catch (Exception e) {
            logger.error("execCmd error", e);
            exitValue = -1;
        }
        return exitValue;
    }

    class StreamRunnable implements Runnable {

        private Reader reader = null;

        private BufferedReader br = null;

        List<String> lines = null;

        public StreamRunnable(InputStream is, List<String> lines) {
            Reader reader = new InputStreamReader(is);
            br = new BufferedReader(reader);
            this.lines = lines;
        }

        @Override
        public void run() {
            try {
                String line = br.readLine();
                while (StringUtils.isNotBlank(line)) {
                    logger.info(line);
                    if (lines != null)
                        lines.add(line);
                    line = br.readLine();
                }
            } catch (Exception e) {
                logger.error("stream runnable error", e);
            } finally {
                close(br);
                close(reader);
            }
        }

        private void close(Closeable closeable) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
