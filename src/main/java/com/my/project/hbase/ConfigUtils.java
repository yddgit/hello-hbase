package com.my.project.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ConfigUtils {

	private static Logger LOGGER = Logger.getLogger(ConfigUtils.class);
	private static Properties config;

	static {
		loadConfig();
	}

	private static void loadConfig() {
		try {
			config = new Properties();
			config.load(ConfigUtils.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (IOException e) {
			LOGGER.error("Load config.properties error", e);
		}
	}

	public static String getValue(String name) {
		if(config == null) { loadConfig(); }
		return config.getProperty(name);
	}

	public static String getZookeeper() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.zookeeper.quorum");
	}

	public static String getPort() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.zookeeper.property.clientPort");
	}

	public static String getTableName() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.table.name");
	}

	public static String getColumnFamily() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.table.column.family");
	}

	public static String getFilterQualifier() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.table.column.value.filter.qualifier");
	}

	public static String getFilterValue() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.table.column.value.filter.value");
	}

	public static String getQualifierFilter() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.table.qualifier.filter");
	}

	public static String getTimeout() {
		if(config == null) { loadConfig(); }
		return config.getProperty("hbase.rpc.timeout");
	}

}
