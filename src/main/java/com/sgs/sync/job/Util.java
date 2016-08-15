/**
 * 
 */
package com.sgs.sync.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Sreenivasa.Raogopisetti
 *
 */
public class Util {

	public static Map<String, String> tableMapping = new LinkedHashMap<>();
	private static Map<String, Properties> columnMappings = new HashMap<>();
	public static List<String> deleteOrder = new ArrayList<>();
	public static Properties cloudDBconfigs = new Properties();
	public static Properties localDBconfigs = new Properties();
	public static Properties configurations = new Properties();
	
	
	private void config() {
		tableMapping = readTableProperties("tables.properties");
		for (Object obj : tableMapping.keySet()) {
			String key = obj.toString();
			columnMappings.put(key, readProperties(key + ".properties"));
		}
	}

	public void init(String configFileName) {
		configurations = readProperties(configFileName);
		if(configurations.isEmpty()){
			throw new RuntimeException("Failed to configure database, verify configuration file exist and correct");
		}
		config();
	}
	
	public static String getTableMapped(String key) {
		return tableMapping.get(key);
	}

	public static String getColumnMapped(String tableName, String columnName) {
		return columnMappings.get(tableName).getProperty(columnName);
	}

	private static Properties readProperties(String fileName) {
		try (InputStream in = Util.class.getClassLoader().getResourceAsStream("" + fileName)) {
			Properties prop = new Properties();
			prop.load(in);
			return prop;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public  Map<String, String> readTableProperties(String fileName) {
		Map<String, String> ordered = new LinkedHashMap<>();
		InputStream inputStream = null;  
		try {
			inputStream = ClassLoader.getSystemResourceAsStream(fileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.trim().contains("=")) {
					String[] keyValue = line.split("=");
					if (keyValue.length == 2) {
						ordered.put(keyValue[0], keyValue[1]);
						deleteOrder.add(keyValue[0]);
					}
				}
			}
			Collections.reverse(deleteOrder);
			inputStream.close();
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
		return ordered;
	}

	public static void main(String[] args) throws IOException {
		Util util = new Util();
		util.config();

	}
}
