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
import com.creditease.dbus.bean.DataSourceValidateBean;
import com.creditease.dbus.bean.TopologyStartUpBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.TopologyType;
import com.creditease.dbus.domain.mapper.DataSchemaMapper;
import com.creditease.dbus.domain.mapper.DataSourceMapper;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.domain.mapper.NameAliasMappingMapper;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.NameAliasMapping;
import com.creditease.dbus.domain.model.TopologyJar;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.source.MongoSourceFetcher;
import com.creditease.dbus.service.source.SourceFetcher;
import com.creditease.dbus.util.TranscodeUtils;
import com.creditease.dbus.util.XLSX2CSV;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * User: 王少楠
 * Date: 2018-05-07
 * Time: 下午5:34
 */
@Service
public class DataSourceService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSourceMapper mapper;
    @Autowired
    private DataSchemaMapper schemaMapper;
    @Autowired
    private DataTableMapper tableMapper;
    @Autowired
    private JarManagerService jarManagerService;
    @Autowired
    private NameAliasMappingMapper nameAliasMappingMapper;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;
    private static final Pattern IP_PATTERN = Pattern.compile("(2(5[0-5]{1}|[0-4]\\d{1})|[0-1]?\\d{1,2})(\\.(2(5[0-5]{1}|[0-4]\\d{1})|[0-1]?\\d{1,2})){3}");
    private static final Pattern MYSQL_URL_PATTERN = Pattern.compile("jdbc:mysql://([\\w-\\d\\\\.]+):(\\d+)/.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_URL_PATTERN = Pattern.compile("HOST\\s*=\\s*([\\w-\\d\\\\.]+)\\)\\(PORT\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final String[] TEMPLATE_FILE_HEADER = {"type", "ip", "port", "schema", "table"};
    private static final String[] RETURN_FILE_HEADER = {"type", "ip", "port", "schema", "table", "datasource", "exist in dbus"};

    /*数据源检查的结果 */
    private static final int VALIDATE_OK = 1;
    private static final int VALIDATE_ERROR_TYPE = 0;
    private static final int VALIDATE_ERROR = -1;

    public static final int DATASOURCE_EXISTS = -1;

    /**
     * resource页的搜索
     *
     * @param dsName 若为空,则返回所有DataSource信息
     * @return
     */
    public PageInfo<Map<String, Object>> search(int pageNum, int pageSize, String dsName,
                                                String sortBy, String order, String dsType) {
        Map<String, Object> param = new HashedMap();
        param.put("dsName", dsName == null ? dsName : dsName.trim());
        param.put("sortBy", sortBy == null ? sortBy : sortBy.trim());
        param.put("dsType", dsType == null ? dsType : dsType.trim());
        param.put("order", order);

        PageHelper.startPage(pageNum, pageSize);
        List<Map<String, Object>> dataSources = mapper.search(param);
        try {
            // 获取各数据线Topology运行情况
            stormTopoHelper.populateToposToDs(dataSources);
        } catch (Exception e) {
            logger.warn("Get Storm Rest API failed. Can't access Topology running status!", e);
        }
        return new PageInfo<>(dataSources);
    }

    /**
     * @param id 数据库中的primary  key
     */
    public DataSource getById(Integer id) {
        DataSource dataSource = mapper.selectByPrimaryKey(id);
        NameAliasMapping nameAliasMapping = nameAliasMappingMapper.selectByNameId(NameAliasMapping.datasourceType, id);
        if (nameAliasMapping != null) {
            dataSource.setDsNameAlias(nameAliasMapping.getAlias());
        }
        return dataSource;
    }

    /**
     * 根据 ID更新某条记录
     *
     * @param updateOne
     */
    public int update(DataSource updateOne) {
        updateOne.setUpdateTime(new Date());
        if (updateOne.getStatus().equals("inactive")) {
            schemaMapper.inactiveSchemaByDsId(updateOne.getId());
            tableMapper.inactiveTableByDsId(updateOne.getId());
        }
        String dsNameAlias = updateOne.getDsNameAlias();
        if (StringUtils.isNotBlank(dsNameAlias)) {
            NameAliasMapping nameAliasMapping = nameAliasMappingMapper.selectByNameId(NameAliasMapping.datasourceType, updateOne.getId());
            if (nameAliasMapping == null) {
                nameAliasMapping = new NameAliasMapping();
                nameAliasMapping.setType(NameAliasMapping.datasourceType);
                nameAliasMapping.setAlias(dsNameAlias);
                nameAliasMapping.setName(updateOne.getDsName());
                nameAliasMapping.setNameId(updateOne.getId());
                nameAliasMapping.setUpdateTime(new Date());
                nameAliasMappingMapper.insert(nameAliasMapping);
                logger.info("数据线{}新增别名配置", updateOne.getDsName());
            } else {
                nameAliasMapping.setAlias(dsNameAlias);
                nameAliasMapping.setName(updateOne.getDsName());
                nameAliasMapping.setNameId(updateOne.getId());
                nameAliasMapping.setUpdateTime(new Date());
                nameAliasMappingMapper.updateByPrimaryKey(nameAliasMapping);
                logger.info("数据线{}新增别名配置", updateOne.getDsName());
            }
        }
        return mapper.updateByPrimaryKey(updateOne);
    }

    /**
     * @return 插入的新数据的ID
     */
    public int insertOne(DataSource newOne) {
        //根据dsName检查是否存在,存在就更新
        DataSource dataSource = getByName(newOne.getDsName());
        if (dataSource != null) {
            logger.info("[add dataSource] DataSource exists: {}", newOne.getDsName());
            newOne.setId(dataSource.getId());
            newOne.setUpdateTime(new Date());
            mapper.updateByPrimaryKey(newOne);
            return newOne.getId();
        }
        newOne.setUpdateTime(new Date());
        mapper.insert(newOne);
        return newOne.getId();
    }

    /**
     * 根据ID直接从数据库里删除该条数据
     *
     * @param id
     */
    public int deleteById(Integer id) {
        return mapper.deleteByPrimaryKey(id);
    }


    /**
     * 根据数据源名称查询数据源
     *
     * @param dsName 数据源名称,不存在则查询所有数据源
     * @return 返回满足条件的数据源列表
     */
    public List<DataSource> getDataSourceByName(String dsName) {
        HashMap<String, Object> param = new HashMap<>();
        param.put("dsName", StringUtils.trim(dsName));
        return mapper.getDataSourceByName(param);
    }

    public void modifyDataSourceStatus(Long id, String status) {
        HashMap<String, Object> param = new HashMap<>();
        param.put("id", id);
        param.put("status", status);
        mapper.updateDsStatusByPrimaryKey(param);
    }

    public List<Map<String, Object>> getDSNames() {
        return mapper.getDSNames();
    }

    /**
     * 检查数据源的信息是否正确(能否连通)
     *
     * @return MessageCode
     */
    public int validate(DataSourceValidateBean dsInfo) {
        Map<String, Object> masterParam = buildValidateParam(dsInfo, true);//获取master的参数
        Map<String, Object> slaverParam = buildValidateParam(dsInfo, false);//获取slaver的参数
        try {
            int masterRes = doValidateParam(masterParam);
            int slaveRes = doValidateParam(slaverParam);

            if (masterRes == VALIDATE_OK && slaveRes == VALIDATE_OK) {
                return MessageCode.OK;
            } else {
                return MessageCode.DATASOURCE_VALIDATE_FAILED;
            }
        } catch (Exception e) {
            logger.error("[validate datasource info]dsInfo:{}, Exception: {}", dsInfo.toString(), e);
            throw e;
        }

    }

    private int doValidateParam(Map<String, Object> map) {
        try {
            DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString());
            if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                    || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)
                    || dsType.equals(DbusDatasourceType.LOG_UMS)
                    || dsType.equals(DbusDatasourceType.LOG_FLUME)
                    || dsType.equals(DbusDatasourceType.LOG_FILEBEAT)
                    || dsType.equals(DbusDatasourceType.LOG_JSON)) {
                return VALIDATE_OK;//1;
            } else if (dsType.equals(DbusDatasourceType.MYSQL)
                    || dsType.equals(DbusDatasourceType.ORACLE)
                    || dsType.equals(DbusDatasourceType.DB2)
                    ) {
                SourceFetcher fetcher = SourceFetcher.getFetcher(map);
                int temp = fetcher.fetchSource(map);
                return temp;
            } else if (dsType.equals(DbusDatasourceType.MONGO)) {
                MongoSourceFetcher fetcher = new MongoSourceFetcher();
                int temp = fetcher.fetchSource(map);
                return temp;
            }
            return VALIDATE_ERROR_TYPE;

        } catch (Exception e) {
            logger.error("[datasource validate param]Error encountered, parameter:{}", JSON.toJSONString(map), e);
            return VALIDATE_ERROR;
        }
    }


    /**
     * 构造检查参数
     *
     * @param ifMaster master 或 slave
     */
    private Map<String, Object> buildValidateParam(DataSourceValidateBean rowParam, boolean ifMaster) {
        Map<String, Object> validateParam = new HashedMap(4);
        validateParam.put("dsType", rowParam.getDsType());
        validateParam.put("user", rowParam.getUser());
        validateParam.put("password", rowParam.getPwd());
        //根据传入的不同类型,构造不同的结果
        if (ifMaster) {
            validateParam.put("URL", rowParam.getMasterUrl());
        } else {
            validateParam.put("URL", rowParam.getSlaveUrl());
        }
        return validateParam;
    }

    /**
     * 添加数据源
     *
     * @return dsId, 根据dsID进行后续的操作。
     */
    public Integer addDataSource(DataSource newDS) {
        mapper.insert(newDS);
        return newDS.getId();
    }

    public List<TopologyStartUpBean> getPath(int dsId) {
        try {
            DataSource dataSource = getById(dsId);
            String dsType = dataSource.getDsType();
            //1.根据dsType--->得到需要启动的topologyType s
            //2.根据topologyType--->得到对应的jar信息列表
            //3.从jar信息列表中--->获取最新的jar版本
            //4.构造返回结果
            //根据dsType--->得到需要启动的topologyType列表
            List<TopologyType> topologyTypes = getTopologyTypesByDsType(dsType);
            if (topologyTypes == null) {
                throw new Exception("datasource dsType error");
            }

            List<TopologyStartUpBean> result = new ArrayList<>(topologyTypes.size()); //定义接口返回的结果

            /*根据topologyType,得到相应的jar信息,然后构造返回信息*/
            for (TopologyType topologyType : topologyTypes) {
                List<TopologyJar> topologyJars = jarManagerService.queryJarInfos(null, null, topologyType.getValue());
                //根据排序结果,获取最新的jar信息,然后构造返回结果
                if (topologyJars.isEmpty()) {
                    continue;
                }
                TopologyJar topologyJar = topologyJars.get(0);
                TopologyStartUpBean topologyStartUpBean = new TopologyStartUpBean();
                topologyStartUpBean.setDsName(dataSource.getDsName());
                String topologyTypeStr = topologyType.getValue().replace('_', '-'); //将路径的的下划线换成名称的短横线
                topologyStartUpBean.setTopolotyType(topologyTypeStr);
                topologyStartUpBean.setTopolotyName(dataSource.getDsName() + '-' + topologyTypeStr);
                topologyStartUpBean.setStatus("inactive");
                topologyStartUpBean.setJarName(topologyJar.getName());
                topologyStartUpBean.setJarPath(topologyJar.getPath());
                //加入返回结果集
                result.add(topologyStartUpBean);
            }

            return result;
        } catch (Exception e) {
            logger.error("[get path] DsId:{}, Exception:{}", dsId, e);
            return null;
        }
    }

    private static Float versionToFloat(String version) {
        String digitalStr = version.substring(0, version.lastIndexOf("."));
        float digital = Float.valueOf(digitalStr);
        return digital;
    }

    /**
     * 根据数据源的dsType,判断需要返回的type信息
     */
    private List<TopologyType> getTopologyTypesByDsType(String dsType) {
        DbusDatasourceType dbusDatasourceType = DbusDatasourceType.parse(dsType);
        List<TopologyType> topologyTypes = new ArrayList<>();
        if (dbusDatasourceType == DbusDatasourceType.MYSQL) {
            topologyTypes.add(TopologyType.DISPATCHER_APPENDER);
            topologyTypes.add(TopologyType.SPLITTER_PULLER);
            //topologyTypes.add(TopologyType.MYSQL_EXTRACTOR);

        } else if (dbusDatasourceType == DbusDatasourceType.ORACLE || dbusDatasourceType == DbusDatasourceType.MONGO) {
            topologyTypes.add(TopologyType.DISPATCHER_APPENDER);
            topologyTypes.add(TopologyType.SPLITTER_PULLER);
        } else if (dbusDatasourceType == DbusDatasourceType.DB2) {
            topologyTypes.add(TopologyType.DISPATCHER_APPENDER);
            topologyTypes.add(TopologyType.SPLITTER_PULLER);
        } else if (dbusDatasourceType == DbusDatasourceType.LOG_FILEBEAT ||
                dbusDatasourceType == DbusDatasourceType.LOG_FLUME ||
                dbusDatasourceType == DbusDatasourceType.LOG_LOGSTASH ||
                dbusDatasourceType == DbusDatasourceType.LOG_LOGSTASH_JSON ||
                dbusDatasourceType == DbusDatasourceType.LOG_JSON ||
                dbusDatasourceType == DbusDatasourceType.LOG_UMS) {
            topologyTypes.add(TopologyType.LOG_PROCESSOR);
        } else {
            return null;
        }
        return topologyTypes;
    }

    public DataSource getByName(String dsName) {
        return mapper.getByName(StringUtils.trim(dsName));
    }

    public List<DataSource> getDataSourceByDsType(String dsType) {
        return mapper.getDataSourceByDsType(dsType);
    }

    public List<DataSource> getDataSourceByDsTypes(List<String> dsTypes) {
        return mapper.getDataSourceByDsTypes(dsTypes);
    }

    public String searchDatasourceExist(String ip, int port) throws Exception {
        String param = getIpByHostName(ip) + ":" + port;
        String dsName = null;
        List<DataSource> dataSources = mapper.selectAll();
        List<DataSource> dataSourceList = dataSources.stream().filter(dataSource -> dataSource.getDsType().equals("mysql")
                || dataSource.getDsType().equals("oracle")).collect(Collectors.toList());
        for (DataSource dataSource : dataSourceList) {
            try {
                String master = getDbRealAddress(dataSource.getMasterUrl(), dataSource.getDsType());
                String slave = getDbRealAddress(dataSource.getSlaveUrl(), dataSource.getDsType());
                if (master.contains(param) || slave.contains(param)) {
                    if (dsName != null) {
                        dsName += ",";
                    } else {
                        dsName = dataSource.getDsName();
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return dsName;
    }

    private String getDbRealAddress(String url, String dsType) throws Exception {
        Matcher matcher;
        String host;
        String port;
        String result = "";
        if (dsType.equalsIgnoreCase("oracle")) {
            matcher = ORACLE_URL_PATTERN.matcher(url);
        } else {
            matcher = MYSQL_URL_PATTERN.matcher(StringUtils.lowerCase(url));
        }
        while (matcher.find()) {
            host = matcher.group(1);
            port = matcher.group(2);
            result = result + getIpByHostName(host) + ":" + port + ",";
        }
        return result.substring(0, result.length() - 1);
    }

    public String getIpByHostName(String host) throws Exception {
        if (StringUtils.isBlank(host)) {
            return null;
        }
        Matcher ipMatcher = IP_PATTERN.matcher(host);
        if (!ipMatcher.matches()) {
            try {
                InetAddress address = InetAddress.getByName(host);
                host = address.getHostAddress();
            } catch (Exception e) {
                logger.error("getDBusDataSourceInfo getIpByHostName", e);
                throw e;
            }
        }
        return host;
    }

    public List<File> generateAddTableSql(File file) throws Exception {
        List<File> files = new ArrayList<>();
        Connection conn = null;
        File csvFile = null;
        File retFile = null;
        File retFileTranscode = null;
        logger.info("generate add table sql start.");

        try {
            csvFile = new File(file.getParentFile(), file.getName() + ".csv");
            XLSX2CSV xlsx2CSV = new XLSX2CSV(file.getAbsolutePath(), csvFile.getAbsolutePath());
            xlsx2CSV.process();
            logger.info("xlsx to csv success. file:{}", csvFile.getAbsolutePath());

            String fileName = "加表脚本";
            retFile = new File(file.getParentFile(), fileName + ".csv");
            retFile.createNewFile();

            List<Map<String, String>> csvData = obtainCsvData(csvFile);
            logger.info("load csv file data success.");

            HashMap<String, String> dsNames = searchDsNameByIpPort(csvData);
            logger.info("get datasource success.{}", dsNames);

            // 判断表在dbus是否接过
            checkTableExistInDbus(csvData, dsNames);
            logger.info("check table does it exist in dbus completed.");

            // 生成结果csv
            writeResultCsv(csvData, retFile);
            logger.info("generate csv result file completed.");
            logger.debug("final csv data: {}", JSON.toJSONString(csvData));

            retFileTranscode = new File(file.getParentFile(), fileName + "-result.csv");
            TranscodeUtils.transcode(retFile, retFileTranscode, "utf-8", "gbk");
            files.add(retFileTranscode);

            List<File> scriptFiles = writeScriptFile(csvData, file.getParentFile());
            logger.info("generate script sql file completed.");
            files.addAll(scriptFiles);
        } catch (Exception e) {
            throw e;
        } finally {
            DbUtils.closeQuietly(conn);
            if (csvFile != null)
                csvFile.delete();
            if (retFile != null)
                retFile.delete();
        }

        logger.info("generate add table sql end.");
        return files;
    }

    private List<File> writeScriptFile(List<Map<String, String>> csvData, File dir) throws Exception {
        List<File> files = new ArrayList<>();
        List<String> grant = new ArrayList<>();
        List<String> supplementalLog = new ArrayList<>();
        List<String> oggSource = new ArrayList<>();

        for (Map<String, String> map : csvData) {
            try {
                String type = map.get("type");
                if (StringUtils.equalsIgnoreCase(map.get("exist in dbus"), "no")) {
                    String schema = map.get("schema");
                    String table = map.get("table");
                    grant.add(String.format("GRANT SELECT ON %s.%s TO DBUS;", schema, table).toUpperCase());
                    if (StringUtils.equalsIgnoreCase(type, "oracle")) {
                        supplementalLog.add(String.format("ALTER TABLE %S.%S ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;", schema, table).toUpperCase());
                        oggSource.add(String.format("TABLE %s.%s;", schema, table).toUpperCase());
                    }
                } else {
                    logger.info("table exist in dbus .{}", map);
                }
            } catch (Exception e) {
                logger.error("execute writeScriptFile error", e);
            }
        }

        BufferedWriter grantBw = null;
        BufferedWriter oggBw = null;
        try {
            if (!grant.isEmpty()) {
                File grantFile = new File(dir, "grant.sql");
                files.add(grantFile);
                grantBw = new BufferedWriter(new FileWriter(grantFile));
                for (String line : grant) {
                    grantBw.write(line);
                    grantBw.newLine();
                }
                if (!supplementalLog.isEmpty()) {
                    for (String line : supplementalLog) {
                        grantBw.write(line);
                        grantBw.newLine();
                    }
                }
            }
            if (!oggSource.isEmpty()) {
                File oggFile = new File(dir, "ogg.txt");
                files.add(oggFile);
                oggBw = new BufferedWriter(new FileWriter(oggFile));
                for (String line : oggSource) {
                    oggBw.write(line);
                    oggBw.newLine();
                }
            }
        } catch (Exception e) {
            logger.error("writeScriptOrSqlFile error.", e);
            throw e;
        } finally {
            IOUtils.closeQuietly(grantBw);
            IOUtils.closeQuietly(oggBw);
        }
        return files;
    }

    private void writeResultCsv(List<Map<String, String>> csvData, File retFile) throws Exception {
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        CSVPrinter csvPrinter = null;
        try {
            fos = new FileOutputStream(retFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);

            CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(RETURN_FILE_HEADER);
            csvPrinter = new CSVPrinter(bw, csvFormat);
            for (Map<String, String> map : csvData) {
                List<String> record = new ArrayList<>();
                for (String name : RETURN_FILE_HEADER) {
                    record.add(map.get(name));
                }
                csvPrinter.printRecord(record);
            }
            bw.flush();
            fos.flush();
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
            if (csvPrinter != null)
                csvPrinter.close();
        }
    }

    private void checkTableExistInDbus(List<Map<String, String>> csvData, HashMap<String, String> dsNames) {
        HashSet<String> set = new HashSet<>();
        set.addAll(dsNames.values());
        List<DataTable> tables = tableMapper.findTablesByDsNames(set);
        ArrayList<String> list = new ArrayList<>();
        for (DataTable table : tables) {
            list.add(StringUtils.join(new String[]{table.getDsName(), table.getSchemaName(), table.getTableName()}, ".").toLowerCase());
        }
        for (Map<String, String> csvDatum : csvData) {
            if (StringUtils.isNotBlank(csvDatum.get("datasource"))) {
                for (String datasource : StringUtils.split(csvDatum.get("datasource"), ",")) {
                    if (list.contains(StringUtils.join(new String[]{datasource, csvDatum.get("schema"), csvDatum.get("table")}, ".").toLowerCase())) {
                        csvDatum.put("exist in dbus", "yes");
                        break;
                    } else {
                        csvDatum.put("exist in dbus", "no");
                    }
                }
            } else {
                csvDatum.put("exist in dbus", "no");
            }
        }
    }

    private HashMap<String, String> searchDsNameByIpPort(List<Map<String, String>> csvData) throws Exception {
        Set<String> ipPortSet = csvData.stream().map(map -> map.get("ip") + ":" + map.get("port")).collect(Collectors.toSet());
        HashMap<String, String> dsNames = new HashMap<>();
        for (String ipPort : ipPortSet) {
            String[] split = StringUtils.split(ipPort, ":");
            String dsName = searchDatasourceExist(split[0], Integer.parseInt(split[1]));
            dsNames.put(ipPort, dsName);
        }
        for (Map<String, String> map : csvData) {
            String dsName = dsNames.get(map.get("ip") + ":" + map.get("port"));
            map.put("datasource", dsName);
        }
        return dsNames;
    }

    private List<Map<String, String>> obtainCsvData(File csvFile) throws IOException {
        List<Map<String, String>> csvData = new ArrayList<>();
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        CSVParser csvParser = null;
        try {
            fis = new FileInputStream(csvFile);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);

            CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(TEMPLATE_FILE_HEADER);
            csvParser = new CSVParser(br, csvFormat);

            int idx = 0;
            for (CSVRecord csvRecord : csvParser.getRecords()) {
                if (idx++ == 0)
                    continue;
                Map<String, String> map = new LinkedHashMap<>();
                for (String name : TEMPLATE_FILE_HEADER) {
                    map.put(name, csvRecord.get(name));
                }
                csvData.add(map);
            }
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(fis);
            if (csvParser != null)
                csvParser.close();
        }
        return csvData;
    }

    public ResponseEntity downloadExcleModel(File file) throws Exception {
        if (!file.exists()) {
            String[] header = {"type", "ip", "port", "schema", "table"};
            String[][] context = {{"mysql", "127.0.0.1", "3306", "db1", "table1"},
                    {"oracle", "127.0.0.1", "1521", "db2", "table2"}
            };
            generatXlsx(header, context, file);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + URLEncoder.encode("加表模板.xlsx", "utf8"));
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.length())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new FileSystemResource(file));
    }

    public static void generatXlsx(String[] header, String[][] example, File file) throws Exception {
        OutputStream os = null;
        try {
            // 创建工作文档对象
            Workbook wb = new XSSFWorkbook();
            // 创建sheet对象
            Sheet sheet1 = wb.createSheet("sheet1");
            // 循环写入行数据
            Row row = sheet1.createRow(0);
            for (int i = 0; i < header.length; i++) {
                Cell cell = row.createCell(i);
                cell.setCellValue(header[i]);
            }
            for (int i = 0; i < example.length; i++) {
                row = sheet1.createRow(i + 1);
                // 循环写入列数据
                for (int j = 0; j < example[i].length; j++) {
                    Cell cell = row.createCell(j);
                    cell.setCellValue(example[i][j]);
                }
            }
            // 创建文件流
            os = new FileOutputStream(file);
            // 写入数据
            wb.write(os);
        } finally {
            // 关闭文件流
            if (os != null) {
                os.close();
            }
        }
    }

}
