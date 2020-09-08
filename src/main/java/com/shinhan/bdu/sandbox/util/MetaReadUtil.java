package com.shinhan.bdu.sandbox.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tools.ant.launch.Launcher;
/**
 * 
 * @desc Util Class for read config, sql and pipeline meta.
 *
 */
public class MetaReadUtil {
	private static  MetaReadUtil instance = null;
	private static final String RESOURCE_DIR = "resources";
	private static final String IMPALA_CONFIG_FILE_URI = "impala-config.data";
	private static final String MARIA_CONFIG_FILE_URI = "mariadb-config.data";
	private static final String HDFS_CONFIG_FILE_URI = "hdfs-capa-config.data";
	private static final String HADOOP_CONFIG_FILE_URI = "hadoop-config.data";
	private static final String BASIC_CONFIG_FILE_URI = "basic-config.data";
	private static final String QUERY_DIR_PATH = "query";
	private static final String PRCS_DIR_PATH = "process";

	private MetaReadUtil() {
	}

	synchronized public static MetaReadUtil getInstance() {
		if (instance == null) {
			instance = new MetaReadUtil();
		}
		return instance;
	}

	@SuppressWarnings("static-access")
	public Map<String, String> readImpalaConfig() {
		return readConfig(this.IMPALA_CONFIG_FILE_URI);

	}
	 
	@SuppressWarnings("static-access")
	public Map<String, String> readHdfsConfig() {
		return readConfig(this.HDFS_CONFIG_FILE_URI);

	}
	@SuppressWarnings("static-access")
	public Map<String, String> readMariaConfig() {
		return readConfig(this.MARIA_CONFIG_FILE_URI);
	}
	
	@SuppressWarnings("static-access")
	public Map<String, String> readHadoopConfig() {
		return readConfig(this.HADOOP_CONFIG_FILE_URI);
	}
	
	@SuppressWarnings("static-access")
	public Map<String, String> readBasicConfig() {
		return readConfig(this.BASIC_CONFIG_FILE_URI);
	}
	
	@SuppressWarnings("static-access")
	public Map<String, String> readConfig(String configType) {
		Map<String, String> map = new HashMap<String, String>();
		String targetPath = Paths.get(RESOURCE_DIR, configType).toString();
		for ( String line : FileUtil.getInstance().fileRead(targetPath)) {
			String key = line.substring(0, line.indexOf("="));
			String value = line.substring(line.indexOf("=")+1);
			map.put(key, value);
		}
		return map;

	}
	
	private static String readFile(String path) {
		StringBuffer sb = new StringBuffer();
		String targetPath = Paths.get(RESOURCE_DIR, path).toString();
		for (String line : FileUtil.getInstance().fileRead(targetPath)) {
			sb.append(line + '\n');
		}
		return sb.toString();

	}

	public static String readSql(String sqlFn) {
		return readFile((Paths.get(QUERY_DIR_PATH, (sqlFn + ".sql"))).toString());
	}
	
	public static String readJsonFile(String prcsFn) {
		return readFile((Paths.get(PRCS_DIR_PATH, (prcsFn + ".json"))).toString());
	}
	

}
