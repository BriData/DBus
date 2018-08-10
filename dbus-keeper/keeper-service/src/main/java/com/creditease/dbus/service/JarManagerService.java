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

package com.creditease.dbus.service;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.mapper.EncodePluginsMapper;
import com.creditease.dbus.domain.mapper.ProjectMapper;
import com.creditease.dbus.domain.model.EncodePlugins;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.PluginManager;
import com.creditease.dbus.encoders.PluginScanner;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
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

/**
 * Created by mal on 2018/4/19.
 */
@Service
public class JarManagerService {

    private static Pattern pattern = Pattern.compile("([\\\\/]{1}[\\d\\w\\-_\\.]+){3}[\\\\/]{1}[\\d\\w\\-_\\.]+\\.jar");
    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String DBUS_JARS_BASE_PATH = "dbus.jars.base.path";

    public static final String DBUS_ROUTER_JARS_BASE_PATH = "dbus.router.jars.base.path";

    public static final String DBUS_ENCODE_PLUGINS_JARS_BASE_PATH = "dbus.encode.plugins.jars.base.path";

    public static final String DBUS_KERBEROS_KEYTAB_FILE_BASE_PATH = "dbus.kerberos.keytab.file.base.path";

    @Autowired
    private IZkService zkService;

    @Autowired
    private EncodePluginsMapper encodePluginsMapper;

    @Autowired
    private ProjectMapper projectMapper;

    public List<String> queryVersion(String category) throws Exception {
        String pathKey = DBUS_JARS_BASE_PATH;
        if (StringUtils.equals(category, "router")) pathKey = DBUS_ROUTER_JARS_BASE_PATH;
        String baseDir = obtainBaseDir(pathKey);

        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty("user");
        String stormNimbusHost = props.getProperty("storm.nimbus.host");
        String port = props.getProperty("storm.nimbus.port");

        // 0:user name, 1:nimbus host, 2:ssh port,
        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -maxdepth 1 -a -type d | sed '1d'",
                user, stormNimbusHost, port, baseDir) + " | awk -F '/' '{print $NF}'";
        logger.info("cmd command: {}", cmd);

        List<String> versions = new ArrayList<>();
        execCmd(cmd, versions);
        return versions;
    }

    public List<String> queryType(String category, String version) throws Exception {
        String pathKey = DBUS_JARS_BASE_PATH;
        if (StringUtils.equals(category, "router")) pathKey = DBUS_ROUTER_JARS_BASE_PATH;
        String baseDir = obtainBaseDir(pathKey);
        String sourcePath = StringUtils.join(new String[]{baseDir, version}, File.separator);

        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty("user");
        String stormNimbusHost = props.getProperty("storm.nimbus.host");
        String port = props.getProperty("storm.nimbus.port");

        // 0:user name, 1:nimbus host, 2:ssh port,
        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -maxdepth 1 -a -type d | sed '1d'",
                user, stormNimbusHost, port, sourcePath) + " | awk -F '/' '{print $NF}'";
        logger.info("cmd command: {}", cmd);

        List<String> types = new ArrayList<>();
        execCmd(cmd, types);
        return types;
    }

    public int uploads(String category, String version, String type, MultipartFile jarFile) {
        int success = 1;
        File file = null;
        try {
            String pathKey = DBUS_JARS_BASE_PATH;
            if (StringUtils.equals(category, "router")) pathKey = DBUS_ROUTER_JARS_BASE_PATH;

            file = new File(SystemUtils.getUserDir(), System.currentTimeMillis() + ".jar");
            jarFile.transferTo(file);

            String baseDir = obtainBaseDir(pathKey);
            long time = System.currentTimeMillis();
            String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
            String savePath = StringUtils.join(new String[]{baseDir, version, type, minorVersion}, File.separator);

            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty("user");
            String stormNimbusHost = props.getProperty("storm.nimbus.host");
            String port = props.getProperty("storm.nimbus.port");

            // 0:ssh port, 1:user name, 2:nimbus host, 3:path
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} mkdir -pv {3}",
                    port, user, stormNimbusHost, savePath);
            logger.info("mdkir command: {}", cmd);
            int retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("mkdir success.");
                // 0:ssh port, 1:source path, 2:user name, 3:nimbus host, 4:dest path
                cmd = MessageFormat.format("scp -P {0} {1} {2}@{3}:{4}",
                        port, file.getPath(), user, stormNimbusHost, savePath + File.separator + jarFile.getOriginalFilename());
                logger.info("scp command: {}", cmd);
                retVal = execCmd(cmd, null);
                if (retVal == 0) {
                    logger.info("scp success.");
                }
            }

        } catch (Exception e) {
            success = 0;
        } finally {
            if (file != null) file.delete();
        }
        return success;
    }

    public int delete(String category, String version, String type, String minorVersion, String fileName) {
        int success = 1;
        try {
            String pathKey = DBUS_JARS_BASE_PATH;
            if (StringUtils.equals(category, "router")) pathKey = DBUS_ROUTER_JARS_BASE_PATH;
            String baseDir = obtainBaseDir(pathKey);
            String parentPath = StringUtils.join(new String[]{baseDir, version, type, minorVersion}, File.separator);
            String filePath = StringUtils.join(new String[]{parentPath, fileName}, File.separator);

            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty("user");
            String stormNimbusHost = props.getProperty("storm.nimbus.host");
            String port = props.getProperty("storm.nimbus.port");

            // 0:ssh port, 1:user name, 2:nimbus host, 3:path
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} rm -rf {3} {4}",
                    port, user, stormNimbusHost, filePath, parentPath);
            logger.info("mdkir command: {}", cmd);
            int retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("rm success.");
            }
        } catch (Exception e) {
            success = 0;
        }
        return success;
    }

    public int batchDelete(List<Map<String, String>> records) {
        int ret = 0;
        for (Map<String, String> record : records) {
            ret += delete(record.get("category"), record.get("version"), record.get("type"), record.get("minorVersion"), record.get("fileName"));
        }
        return ret;
    }

    public List<Map<String, String>> queryJarInfos(String category, String version, String type) throws Exception {
        List<Map<String, String>> jarInfos = new ArrayList<>();
        String pathKey = DBUS_JARS_BASE_PATH;
        if (StringUtils.equals(category, "router")) pathKey = DBUS_ROUTER_JARS_BASE_PATH;
        String baseDir = obtainBaseDir(pathKey);

        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty("user");
        String stormNimbusHost = props.getProperty("storm.nimbus.host");
        String port = props.getProperty("storm.nimbus.port");

        // 0:user name, 1:nimbus host, 2:ssh port
        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -type f -a -name \"*.jar\"",
                user, stormNimbusHost, port, baseDir);
        logger.info("cmd command: {}", cmd);

        List<String> jars = new ArrayList<>();
        int retVal = execCmd(cmd, jars);
        if (retVal == 0) {
            logger.info("obtain jars info success.");
            Map<String, Map<String, String>> records = new TreeMap<>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2) * -1;
                }
            });
            for (String jarPath : jars) {
                Map<String, String> record = new HashMap<>();
                Matcher m = pattern.matcher(jarPath);
                if (m.find()) {
                    String[] arrs = m.group().split("[\\\\/]");
                    record.put("version", arrs[1]);
                    record.put("type", arrs[2]);
                    record.put("minorVersion", arrs[3]);
                    record.put("fileName", arrs[4]);
                    record.put("path", jarPath);
                    if (StringUtils.isBlank(version) && StringUtils.isBlank(type)) {
                        records.put(arrs[3], record);
                    } else if (StringUtils.isNotBlank(version) &&
                            StringUtils.isNotBlank(type) &&
                            StringUtils.contains(arrs[1], version) &&
                            StringUtils.contains(arrs[2], type)) {
                        records.put(arrs[3], record);
                    } else if (StringUtils.isNotBlank(version) &&
                            StringUtils.isBlank(type) &&
                            StringUtils.contains(arrs[1], version)) {
                        records.put(arrs[3], record);
                    } else if (StringUtils.isNotBlank(type) &&
                            StringUtils.isBlank(version) &&
                            StringUtils.contains(arrs[2], type)) {
                        records.put(arrs[3], record);
                    }
                }
            }

            for (Map.Entry<String, Map<String, String>> entry : records.entrySet()) {
                jarInfos.add(entry.getValue());
            }
        }
        return jarInfos;
    }

    private String obtainBaseDir(String proName) throws Exception {
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        return props.getProperty(proName);
    }

    public int uploadsEncodePlugin(String name, Integer projectId, MultipartFile jarFile) throws Exception {
        int retVal = 0;
        File file = null;
        //C:/dbus_encoder_plugins_jars/0/20180809_145823/encoder-plugins-0.5.0.jar
        try {
            String pathKey = DBUS_ENCODE_PLUGINS_JARS_BASE_PATH;

            file = new File(SystemUtils.getUserDir(), System.currentTimeMillis() + ".jar");
            jarFile.transferTo(file);

            String baseDir = obtainBaseDir(pathKey);
            long time = System.currentTimeMillis();
            String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
            String savePath = StringUtils.join(new String[]{baseDir, projectId.toString(), minorVersion}, File.separator);

            Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String user = props.getProperty("user");
            String stormNimbusHost = props.getProperty("storm.nimbus.host");
            String port = props.getProperty("storm.nimbus.port");

            String destPath = savePath + File.separator + jarFile.getOriginalFilename();
            String cmd = MessageFormat.format("ssh -p {0} {1}@{2} mkdir -pv {3}",
                    port, user, stormNimbusHost, savePath);
            logger.info("mdkir command: {}", cmd);
            retVal = execCmd(cmd, null);
            if (retVal == 0) {
                logger.info("mkdir success.");
                // 0:ssh port, 1:source path, 2:user name, 3:nimbus host, 4:dest path
                cmd = MessageFormat.format("scp -P {0} {1} {2}@{3}:{4}",
                        port, file.getPath(), user, stormNimbusHost, destPath);
                logger.info("scp command: {}", cmd);
                retVal = execCmd(cmd, null);
                if (retVal == 0) {
                    logger.info("scp success.");
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
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (file != null) file.delete();
        }
        return retVal;
    }

    public int uploadsKeytab(Integer projectId, String principal, MultipartFile jarFile) throws Exception {
        String baseDir = obtainBaseDir(DBUS_KERBEROS_KEYTAB_FILE_BASE_PATH);
        long time = System.currentTimeMillis();
        String minorVersion = StringUtils.join(new String[]{DateFormatUtils.format(time, "yyyyMMdd"), DateFormatUtils.format(time, "HHmmss")}, "_");
        String savePath = StringUtils.join(new String[]{projectId.toString(), minorVersion}, File.separator);
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
        String path = encodePlugins.getPath();
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (file.exists()) file.delete();
        if (parentFile != null && parentFile.exists()) parentFile.delete();
        encodePluginsMapper.deleteByPrimaryKey(id);
        return 0;
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
