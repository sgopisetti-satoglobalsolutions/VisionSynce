package com.sgs.sync.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SyncProcesser {

	private static void initialize() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver?\n" + " Include in your library path, please!");
			e.printStackTrace();
			return;
		}
	}
	private static Connection getLocalConnection() {
		return getConnection(Util.configurations.getProperty("db.store.host")+":"+Util.configurations.getProperty("db.store.port"),
				Util.configurations.getProperty("db.store.schema"),
				Util.configurations.getProperty("db.store.username"),
				Util.configurations.getProperty("db.store.password"));
	}
	
	private static Connection getCloudConnection() {
		return getConnection(Util.configurations.getProperty("db.cloud.host")+":"+Util.configurations.getProperty("db.cloud.port"),
				Util.configurations.getProperty("db.cloud.schema"),
				Util.configurations.getProperty("db.cloud.username"),
				Util.configurations.getProperty("db.cloud.password"));
	}
	
	private static Connection getConnection(String url, String schema, String userName, String password) {
		try {
			Connection connection = DriverManager.getConnection("jdbc:postgresql://"+url+"/"+schema, userName,password);
			return connection;
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console!");
			e.printStackTrace();
		}
		return null;
	}


	
	private static List<String> formQuery(){
		List<String> tableNames = new ArrayList<String>();
		Connection connection = null;
		try {
			connection = getLocalConnection();
			DatabaseMetaData dbmd = connection.getMetaData();
			String[] types = {"TABLE"};
			
			ResultSet rs = dbmd.getTables(null, null, "%", types);
			while (rs.next()) {				
				String tableName = rs.getString(3);
				tableNames.add(tableName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(connection);
		}
		return tableNames;
	}
	
	private static void columnNames(String table, String mappedTable,int locationId){
		Connection connection = null;
		Statement stmt = null ;
		try{
			connection = getLocalConnection();
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM  "+table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();

			StringBuffer head = new StringBuffer();
			head.append("insert into "+mappedTable+" ( ");
			
			for (int i = 1; i <= columnCount; i++ ) {
			  String name = rsmd.getColumnName(i);
				if(name.contains("location_id")){
					//Special case
					continue;
				}
			  head.append(name);
			  if(i != columnCount)head.append(",");
			}
			head.append(", location_id) values ");
			
			int rows = 0;
			StringBuffer sb = new StringBuffer();
			boolean isEnd = false;
			while(rs.next()) {
				rows ++;
				if (isEnd)sb.append(",");
				sb.append(" ( ");
				for (int i = 1; i <= columnCount; i++ ) {
					String name = rsmd.getColumnName(i);
					if(name.contains("location_id")){
						//Special case
						continue;
					}
					String type = rsmd.getColumnTypeName(i);
					if(type.contains("int")){
						
						String val = null;
						if(type.contains("int4")) {
							rs.getInt(name);
							if(!rs.wasNull()){
								val = rs.getInt(name)+"";
							}			
						}
						if(type.contains("int8")) {
							rs.getLong(name);
							if(!rs.wasNull()){
								val = rs.getLong(name)+"";
							}	
						}
						sb.append(val);
					} else if(type.contains("float")){
						String val = null;
						if(!rs.wasNull()){
							rs.getFloat(name);
							val = rs.getFloat(name)+"";
						}
						sb.append(val);
						
					} else if(type.contains("json")){
						
						sb.append(rs.getString(name)==null ? null : "'"+rs.getString(name)+"'");
						
					} else if(type.contains("serial")){
						
						sb.append(rs.getInt(name));
						
					} else if(type.contains("timestamp")){
						
						sb.append(rs.getTimestamp(name) == null ? null:"'"+rs.getTimestamp(name)+"'");
						
					}else{
						sb.append(rs.getString(name) == null ? null : "'"+rs.getString(name)+"'");
					
					}
					if(i != columnCount)sb.append(",");
				}
				
				sb.append(","+locationId+")");
				isEnd = true;
				if(rows >= 100) {
					rows = 0;
					isEnd = false;
					if( !sb.toString().isEmpty()) {
						sb.append(";");	
						insertIntoCloud(head.toString() + sb.toString());
					}
					sb = new StringBuffer();
				}
				
			}
			if( !sb.toString().isEmpty()) {
				sb.append(";");	
				insertIntoCloud(head.toString() + sb.toString());
			}
			
			
		}catch( Exception ex) {
			ex.printStackTrace();
		}finally{
			close(stmt);
			close(connection);
		}
	}
	
	private static void close(Connection con)  {
		try{
			if( con != null ) {
				con.close();
			}
		}catch(Exception ex) {
			
		}
	}
	
	private static void close(Statement stmt)  {
		try{
			if( stmt != null ) {
				stmt.close();
			}
		}catch(Exception ex) {
			
		}
	}
	
	private static void cleanData(List<String> tableNames, int locationId) {
		try{
			for(String table : tableNames){
				//System.out.println(table);
				cleanData(table,locationId);
			}
		}catch(Exception ex){
			
		}
	}
	
	private static void cleanData(String tableName, int locationId) {
		Connection cloudCon = null;
		Statement stmt =  null;
		try{
			cloudCon = getCloudConnection();
			cloudCon.setAutoCommit(false);
			stmt = cloudCon.createStatement();
			stmt.execute("delete FROM  "+tableName+ " where location_id="+locationId);
			cloudCon.commit();
		}catch(Exception ex){
			try {
				cloudCon.rollback();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ex.printStackTrace();
		}finally{
			close(stmt);
			close(cloudCon);
		}
	}
	
	private static Integer getLocation() {
		Connection cloudCon = null;
		Statement stmt =  null;
		try{
			String query = "select * from location";
			cloudCon = getLocalConnection();
			cloudCon.setAutoCommit(false);
			stmt = cloudCon.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()) {
				int locationId = rs.getInt("id");
				return locationId;
			}
			cloudCon.commit();
		}catch(Exception ex){
			try {
				cloudCon.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ex.printStackTrace();
		}finally{
			close(stmt);
			close(cloudCon);
		}	
		return null;
	}
	
	private static void insertIntoCloud(String inserQuery) {
		Connection cloudCon = null;
		Statement stmt =  null;
		try{
			cloudCon = getCloudConnection();
			cloudCon.setAutoCommit(false);
			stmt = cloudCon.createStatement();
			stmt.execute(inserQuery);
			cloudCon.commit();
		}catch(Exception ex){
			try {
				cloudCon.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			ex.printStackTrace();
		}finally{
			close(stmt);
			close(cloudCon);
		}
	}
	
	public static void main(String[] args) {
		Util util = new Util();
		if(args .length == 0 ) {
			System.out.println("Default configuration file config.properties used ");
			util.init("config.properties");
		} else {
			util.init(args[0]);
		}
		syncProcess();
	}
	
	private static void syncProcess(){
		System.out.println(" Sync process starts "+new Date());
		initialize();
		Integer location = getLocation();
		if ( location != null ) {
			List<String> tableNames = formQuery();
			while ( true ) {
				System.out.println(" Sync cycle starts "+new Date());
				cleanData(Util.deleteOrder, location);
				try {
					for(String key : Util.tableMapping.keySet()){
						if(tableNames.contains(key)) {	
							columnNames(Util.tableMapping.get(key),key,location);
						}
					}
					System.out.println(" Sync cycle ends "+new Date());
					Thread.sleep(getFrequency());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}	
				
			}
		} else {
			System.out.println(" Location not identified !");
		}
		System.out.println(" Sync process ends "+new Date());
	}
	
	private static long getFrequency(){
		return Integer.parseInt(Util.configurations.getProperty("job.frequency")) * 1000;
	}
	
	@SuppressWarnings("unused")
	private static void initPreparation(){
		List<String> tableNames = formQuery();
		for(String table : tableNames){
			columnNames1(table);
		}
	}
	
	private static void columnNames1(String table){
		Connection connection = getLocalConnection();
		Statement stmt ;
		try{
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM  "+table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();

			String cont = "";
			for (int i = 1; i <= columnCount; i++ ) {
			  String name = rsmd.getColumnName(i);
			  name = name +"="+name;
			  cont = cont + name+"\n";
			}
			write(table,cont);
		}catch( Exception ex) {
			
		}
	}
	private static void write(String tableName, String props){
		try {

			File file = new File("/Users/Sreenivasa.Raogopisetti/tmp/dbsync/"+tableName+".properties");

			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(props);
			bw.close();
			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}