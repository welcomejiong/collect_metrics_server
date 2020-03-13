package com.hoolai.bi.collectdata.server.service.mergefile;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.jian.tools.util.JSONUtils;

public class CategoryMergeFileProcesser {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CategoryMergeFileProcesser.class.getSimpleName());

	private File categoryFileDir;
	
	private File currentMergingFile;
	
	private File currentMergingMetaFile;
	
	private MergeMetaInfo currentMergeMetaInfo;

	public CategoryMergeFileProcesser(File categoryFileDir) {
		super();
		this.categoryFileDir = categoryFileDir;
		this.init();
	}
	
	private void init(){
		try {
			this.initCurrentMergingFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void merge(){
		try {
			File[] rowdataFiles=this.getPrepareMergeFile();
			if(rowdataFiles==null||rowdataFiles.length<1){
				return ;
			}
			List<File> candidateFiles = Collections.emptyList();
			candidateFiles = Arrays.asList(rowdataFiles);
			for (File file : candidateFiles) {
				this.doMergeFile(file);
			}
			
			this.copyMergingFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 拷贝正在合并的文件
	 */
	private void copyMergingFile(){
		try {
			// 拷贝正在合并的目录到loader的数据加载目录
			File[] mergedFiles=this.getMergedFiles();
			if(mergedFiles!=null&&mergedFiles.length>0){
				String categoryLoaderPath=this.categoryFileDir.getAbsolutePath().replace(Constant.UPLOAD_PATH, Constant.LOADER_PATH);
				File categoryLoaderFileDir=new File(categoryLoaderPath);
				for (File mergeFile : mergedFiles) {
					FileUtils.copyFileToDirectory(mergeFile, categoryLoaderFileDir, true);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void doMergeFile(File file){
		if(file==null||!file.exists()){
			return ;
		}
		try {
			List<String> lines=FileUtils.readLines(file, "UTF-8");
			if(lines==null||lines.size()<1){
				this.renameMergeSuccFile(file);
				return ;
			}
			List<String> tmpList=new ArrayList<String>();
			long tmpByteLength=0l;
			for (String line : lines) {
				tmpList.add(line);
				tmpByteLength+=line.getBytes().length;
				if(tmpByteLength+this.currentMergeMetaInfo.getBytesLength()>Constant.MERGE_FILE_MAX_LENGTH){
					this.currentMergeMetaInfo.incrBytesLength(tmpByteLength);
					this.currentMergeMetaInfo.setStatus("finished");
					FileUtils.writeLines(this.currentMergingFile, tmpList, true);
					FileUtils.writeStringToFile(this.currentMergingMetaFile, JSONUtils.toJSON(this.currentMergeMetaInfo),"UTF-8",false);
					
					// 把合并后的文件移到loader的数据加载目录
					String categoryLoaderPath=this.categoryFileDir.getAbsolutePath().replace(Constant.UPLOAD_PATH, Constant.LOADER_PATH);
					File categoryLoaderFileDir=new File(categoryLoaderPath);
					File checkTargetFile=new File(categoryLoaderFileDir.getAbsolutePath()+File.separator+this.currentMergingFile.getName());
					if(this.deleteExistFileForWait(checkTargetFile)){
						FileUtils.moveFileToDirectory(this.currentMergingFile, categoryLoaderFileDir, true);
						FileUtils.deleteQuietly(this.currentMergingMetaFile);
					}else{
						LOGGER.info("delete for target move file:"+checkTargetFile.getAbsolutePath()+" fail!");
					}
					
					tmpList.clear();
					// 重新创建一个新的
					this.createNewMergeFile();
					tmpByteLength=0l;
				}
			}
			if(!tmpList.isEmpty()){
				this.currentMergeMetaInfo.incrBytesLength(tmpByteLength);
				FileUtils.writeLines(this.currentMergingFile, tmpList, true);
				FileUtils.writeStringToFile(this.currentMergingMetaFile, JSONUtils.toJSON(this.currentMergeMetaInfo));
				tmpList.clear();
			}
			this.renameMergeSuccFile(file);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private boolean deleteExistFileForWait(File delFile){
		if(!delFile.exists()){
			return true;
		}
		boolean isSucc=delFile.delete();
		return isSucc;
	}
	
	private void renameMergeSuccFile(File file){
		try {
			if(file==null){
				return ;
			}
			String currPath=file.getPath();
			File dest = new File(currPath + ".COMPLATED");
			boolean renamed = file.renameTo(dest);
			if (renamed) {
				LOGGER.debug("Successfully rolled file "+file.getPath()+" to "+dest.getPath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private File[] getPrepareMergeFile(){
		File[] matchFiles=this.categoryFileDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if(name.endsWith("rowdata")){
					return true;
				}
				return false;
			}
		});	
		return matchFiles;
	}
	
	private void initCurrentMergingFile() throws Exception{
		File[] mergedFiles=this.getMergedFiles();
		if(mergedFiles==null||mergedFiles.length<1){
			this.createNewMergeFile();
		}else{
			for (File mergeFile : mergedFiles) {
				boolean isMerging=this.isMergingFile(mergeFile);
				if(!isMerging){
					continue;
				}
				this.currentMergingFile=mergeFile;
				this.currentMergingMetaFile=new File(this.currentMergingFile.getAbsolutePath()+".META");
				this.currentMergeMetaInfo=this.getMergeMetaInfo(this.currentMergingMetaFile);
				break;
			}
			if(this.currentMergingFile==null){
				this.createNewMergeFile();
			}
		}
	}
	
	private boolean isMergingFile(File file){
		try {
			if(!file.exists()){
				return false;
			}
			File metaFile=new File(file.getAbsolutePath()+".META");
			if(!metaFile.exists()){
				return false;
			}
			MergeMetaInfo mergeMetaInfo=this.getMergeMetaInfo(metaFile);
			if("merging".equals(mergeMetaInfo.getStatus())){
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private MergeMetaInfo getMergeMetaInfo(File metaFile) throws Exception{
		if(!metaFile.exists()){
			return null;
		}
		String jsonInfo=FileUtils.readFileToString(metaFile);
		MergeMetaInfo mergeMetaInfo=JSONUtils.fromJSON(jsonInfo, MergeMetaInfo.class);
		return mergeMetaInfo;
	}
	
	/**
	 * 创建一个新的合并文件
	 * @return
	 * @throws Exception 
	 */
	private File createNewMergeFile() throws Exception{
		
		CategoryMetaInfo categoryMetaInfo=null;
		File categoryMetaFile=new File(this.categoryFileDir.getAbsolutePath()+File.separator+this.categoryFileDir.getName()+".META");
		if(categoryMetaFile.exists()){
			String jsonInfo=FileUtils.readFileToString(categoryMetaFile);
			categoryMetaInfo=JSONUtils.fromJSON(jsonInfo, CategoryMetaInfo.class);
		}else{
			categoryMetaFile.createNewFile();
			categoryMetaInfo=new CategoryMetaInfo(0);
			FileUtils.writeStringToFile(categoryMetaFile, JSONUtils.toJSON(categoryMetaInfo), false);
		}
		int fileIdx=categoryMetaInfo.incrAndGet();
		FileUtils.writeStringToFile(categoryMetaFile, JSONUtils.toJSON(categoryMetaInfo), false);
		
		File newMergeFile=new File(this.categoryFileDir.getAbsolutePath()+File.separator+this.categoryFileDir.getName()+"-"+fileIdx+".merged");
		File newMergeMetaFile=new File(this.categoryFileDir.getAbsolutePath()+File.separator+this.categoryFileDir.getName()+"-"+fileIdx+".merged.META");
		boolean isSucc=newMergeFile.createNewFile();
		boolean isMetaSucc=newMergeMetaFile.createNewFile();
		MergeMetaInfo mergeMetaInfo=new MergeMetaInfo("merging", 0l);
		FileUtils.writeStringToFile(newMergeMetaFile, JSONUtils.toJSON(mergeMetaInfo), false);
		this.currentMergingFile=newMergeFile;
		this.currentMergingMetaFile=newMergeMetaFile;
		this.currentMergeMetaInfo=mergeMetaInfo;
		return newMergeFile;
	}
	
	private File[] getMergedFiles(){
		File[] mergedFiles=this.categoryFileDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if(name.endsWith("merged")){
					return true;
				}
				return false;
			}
		});
		return mergedFiles;
	}
	
	private static  class MergeMetaInfo{
		
		private String status;
		
		private long bytesLength;

		public MergeMetaInfo() {
			super();
		}

		public MergeMetaInfo(String status, long bytesLength) {
			super();
			this.status = status;
			this.bytesLength = bytesLength;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public long getBytesLength() {
			return bytesLength;
		}

		public void setBytesLength(long bytesLength) {
			this.bytesLength = bytesLength;
		}
		
		public void incrBytesLength(long length){
			this.bytesLength+=length;
		}
	}
	
	private static class CategoryMetaInfo{
		
		private int idx;

		public CategoryMetaInfo() {
			super();
		}

		public CategoryMetaInfo(int idx) {
			super();
			this.idx = idx;
		}

		public int getIdx() {
			return idx;
		}

		public void setIdx(int idx) {
			this.idx = idx;
		}
		
		public int incrAndGet(){
			return ++idx;
		}
		
		
	}
	
	public class FileComparator implements Comparator<File>{

		@Override
		public int compare(File o1, File o2) {
			long compare = o1.lastModified() - o2.lastModified();
			if (compare == 0) {
				long cl = o1.length() - o2.lastModified();
				if (cl > 0) {
					return 1;
				} else if (cl < 0) {
					return -1;
				}
			} else if (compare > 0) {
				return 1;
			} else {
				return -1;
			}
			return 0;
		}
		
	}
	
	
	
}
