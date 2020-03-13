package com.hoolai.bi.collectdata.server.core;

import java.io.InputStream;
import java.util.Properties;

import org.springframework.util.StringUtils;

import com.hoolai.bi.compress.CompressType;

public class Constant {

	public static final String DEFAULT_FORMAT_DATE_DAY="yyyy-MM-dd";
	
	public static final String DEFAULT_FORMAT_DATE_MINUTES="yyyy-MM-dd HH:mm";
	
	public static final String DEFAULT_FORMAT_DATE_SECONDS="yyyy-MM-dd HH:mm:ss";
	
	public static final String DEFAULT_CHARSET="UTF-8";
	
	public static final int DEFAULT_INDEX_ZEAO = 0;
	
	public static final int DEFAULT_INDEX_FIRST= 1;
	
	// 默认long
	public static long DEFAULT_LONG_ZEAO = 0;
	
	// 默认long
	public static long DEFAULT_LONG_FIRST = 1;
	
	// 默认分隔符
	public static final String DEFAULT_SPLIT = ",";

	// 默认空格
	public static final String DEFAULT_BLANK = " ";

	// 默认下划线
	public static final String DEFAULT_UNDERLINE = "_";

	// 默认斜线
	public static final String DEFAULT_SPRIT = "/";
	
	public static final String CONSTANT_GLOBLE_FILE_PATH = "constant_globle.properties";

	public static final Properties constantProperties=new Properties();
	
	public static String SERVER_URL;
	
	public static String RESOURCE_URL;
	
	public static String SEARCH_INIT_IP;
	
	public static String SITE_NAME;
	
	public static String UPLOAD_PATH;
	
	public static String LOADER_PATH;
	
	public static String MANAGE_AUTH_EXCLUDE_URL;
	
	public static boolean IS_PUBLISHED=false;
	
	public static String VERSION;
	
	public static String[] MONITOR_SYS_EMAILS;
	
	public static CompressType compressType;
	
	public static long MERGE_FILE_MAX_LENGTH;
	
	public static boolean IS_MERGING_SERVER=false;
	
	public static boolean IS_FINAL_DATACENTER=false;
	
	static {
		init();
	}
	
	public static void init(){
		try {
			InputStream globleIn = Constant.class.getClassLoader().getResourceAsStream(CONSTANT_GLOBLE_FILE_PATH);
			Properties globleProperties = new Properties();
			if(globleIn==null){
				throw new RuntimeException("constant_globle.properties is not exists!");
			}
			globleProperties.load(globleIn);
			// 项目本事的常量文件
			String constantFilePath=globleProperties.getProperty("constant_file_path");
			if(StringUtils.isEmpty(constantFilePath)){
				throw new RuntimeException("constant_file_path is not contain in the constant_globle.properties!");
			}
			
			processProjectConstant(constantFilePath);
			
			String recordingConstantFilePath=globleProperties.getProperty("recording_constant_file_path");
			if(StringUtils.isEmpty(recordingConstantFilePath)){
				throw new RuntimeException("recordingConstantFilePath is not contain in the constant_globle.properties!");
			}
			
			org.corps.bi.core.Constants.init(recordingConstantFilePath);
			
			String producerConstantFilePath=globleProperties.getProperty("producer_constant_file_path");
			if(StringUtils.isEmpty(producerConstantFilePath)){
				throw new RuntimeException("producerConstantFilePath is not contain in the constant_globle.properties!");
			}
			
			org.corps.bi.datacenter.producer.core.Constant.init(producerConstantFilePath);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void processProjectConstant(String constantFilePath) throws Exception{
		InputStream constantIn = Constant.class.getClassLoader().getResourceAsStream(constantFilePath);
		constantProperties.load(constantIn);
		
		SERVER_URL=constantProperties.getProperty("server_url");
		RESOURCE_URL=constantProperties.getProperty("resource_url");
		SEARCH_INIT_IP=constantProperties.getProperty("search_init_ip");
		SITE_NAME=constantProperties.getProperty("site_name");
		MANAGE_AUTH_EXCLUDE_URL=constantProperties.getProperty("manage_auth_exclude_url");
		UPLOAD_PATH=constantProperties.getProperty("upload_path");
		LOADER_PATH=constantProperties.getProperty("loader_path");
		IS_PUBLISHED=Boolean.parseBoolean(constantProperties.getProperty("is_published"));
		VERSION=constantProperties.getProperty("version");
		String monitorSysEmails=constantProperties.getProperty("monitor_sys_emails");
		if(!StringUtils.isEmpty(monitorSysEmails)){
			MONITOR_SYS_EMAILS=monitorSysEmails.split(",");
		}
		String compressTypeStr=constantProperties.getProperty("compress_type");
		if(compressTypeStr!=null){
			compressType=CompressType.findByName(compressTypeStr);
		}
		String mergeFileMaxLengthStr=constantProperties.getProperty("merge_file_max_length");
		if(mergeFileMaxLengthStr!=null){
			MERGE_FILE_MAX_LENGTH=Long.parseLong(mergeFileMaxLengthStr);
		}
		String mergingServerStr=constantProperties.getProperty("is_merging_server");
		if(mergingServerStr!=null){
			IS_MERGING_SERVER=Boolean.parseBoolean(mergingServerStr);
		}
		
		String finalDataCenterStr=constantProperties.getProperty("is_final_datacenter");
		if(finalDataCenterStr!=null){
			IS_FINAL_DATACENTER=Boolean.parseBoolean(finalDataCenterStr);
		}
	}
	
}
