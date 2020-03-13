package com.hoolai.bi.collectdata.server.service.mergefile;

import java.io.File;
import java.io.FilenameFilter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hoolai.bi.collectdata.server.core.Constant;

@Component
public class CategoryDataDirectoryChooser {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CategoryDataDirectoryChooser.class.getSimpleName());
	
	private static final DateFormat DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd");
	
	private Date currentDate;
	
	public CategoryDataDirectoryChooser() {
		super();
	}

	public List<File> find(){
		this.currentDate=new Date();
		File baseDir=new File(Constant.UPLOAD_PATH);
		LOGGER.info("find for path:"+baseDir.getAbsolutePath());
		File[] categorys=baseDir.listFiles();
		if(categorys==null||categorys.length<1){
			LOGGER.info("find for path:"+baseDir.getAbsolutePath()+" have no files!");
			return Collections.emptyList();
		}
		List<File> matchDirList=new ArrayList<File>();
		for (File categoryFile : categorys) {
			if(!categoryFile.isDirectory()){
				continue;
			}
			matchDirList.addAll(this.matchCategoryFileDir(categoryFile));
		}
		return matchDirList;
	}
	
	private List<File> matchCategoryFileDir(File categoryFileDir){
		List<File> matchDirList=new ArrayList<File>();
		for (File categoryFile : categoryFileDir.listFiles()) {
			boolean isMatch=this.isMatch(categoryFile);
			if(!isMatch){
				continue;
			}
			LOGGER.info("matchCategoryFileDir for path:"+categoryFile.getAbsolutePath());
			matchDirList.add(categoryFile);
		}
		return matchDirList;
	}
	
	/**
	 * 指标目录下同时满足最近3天，并且有rowdata文件，才满足合并的条件
	 * @param categoryFile
	 * @return
	 */
	private boolean isMatch(File categoryFile){
		try {
			if(this.isDateMatch(categoryFile.getName())&&this.isHaveMergeFiles(categoryFile)){
				LOGGER.info("match for path:"+categoryFile.getAbsolutePath());
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//LOGGER.info("not match for path:"+categoryFile.getAbsolutePath());
		return false;
	}
	
	private boolean isDateMatch(String fileName){
		try {
			String fileDateStr=this.getFileDateStr(fileName);
			
			Date tmpDate=new Date(this.currentDate.getTime());
			for (int i = 0; i < 3; i++) {
				String dateStr=DATE_FORMAT.format(tmpDate);
				tmpDate=DateUtils.addDays(tmpDate, -1);
				if(fileDateStr.equals(dateStr)){
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 目录下有rowdata,才可以合并
	 * @param categoryFile
	 * @return
	 */
	private boolean isHaveMergeFiles(File categoryFile){
		
		File[] matchFiles=categoryFile.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if(name.endsWith("rowdata")){
					return true;
				}
				return false;
			}
		});
		if(matchFiles==null || matchFiles.length<1){
			return false;
		}
		return true;
	}
	
	private String getFileDateStr(String fileName){
		String[] tmp=fileName.split("_");
		return tmp[2];
	}
	
	
	
	

}
