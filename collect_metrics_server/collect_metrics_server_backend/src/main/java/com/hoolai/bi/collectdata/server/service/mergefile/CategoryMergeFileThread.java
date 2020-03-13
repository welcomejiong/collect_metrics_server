package com.hoolai.bi.collectdata.server.service.mergefile;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jian.tools.util.JSONUtils;

public class CategoryMergeFileThread implements Runnable {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CategoryMergeFileThread.class.getSimpleName());
	
	private static final long MAX_LOCK_MILLS=10*60*1000;
	
	private File categoryFileDir;
	
	private File categoryLockMergeFile;

	public CategoryMergeFileThread(File categoryFileDir) {
		super();
		this.categoryFileDir = categoryFileDir;
		this.categoryLockMergeFile=new File(this.categoryFileDir.getAbsolutePath()+File.separator+"merge.LOCK");
	}

	@Override
	public void run() {
		try {
			boolean isGrabSucc=this.grabLock();
			if(!isGrabSucc){
				return ;
			}
			CategoryMergeFileProcesser categoryMergeFileProcesser=new CategoryMergeFileProcesser(categoryFileDir);
			categoryMergeFileProcesser.merge();
			LOGGER.info("merge succ for path:"+categoryFileDir.getAbsolutePath());
			FileUtils.deleteQuietly(categoryLockMergeFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 抢占指标目录锁
	 * @return
	 */
	private boolean grabLock(){
		try {
			Long currThreadId=Thread.currentThread().getId();
			if(this.categoryLockMergeFile.exists()){
				String jsonLockInfo=FileUtils.readFileToString(this.categoryLockMergeFile);
				LockInfo readLockInfo=JSONUtils.fromJSON(jsonLockInfo, LockInfo.class);
				if(!readLockInfo.isExpire()){
					return false;
				}
				readLockInfo.setThreadId(currThreadId);
				readLockInfo.setCreateTime(System.currentTimeMillis());
				FileUtils.writeStringToFile(this.categoryLockMergeFile, JSONUtils.toJSON(readLockInfo), false);
				return true;
			}
			this.categoryLockMergeFile.createNewFile();
			LockInfo lockInfo=new LockInfo(currThreadId, System.currentTimeMillis());
			FileUtils.writeStringToFile(this.categoryLockMergeFile, JSONUtils.toJSON(lockInfo), false);
			String jsonLockInfo=FileUtils.readFileToString(this.categoryLockMergeFile);
			LockInfo readLockInfo=JSONUtils.fromJSON(jsonLockInfo, LockInfo.class);
			if(currThreadId.equals(readLockInfo.getThreadId())){
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private static class LockInfo{
		
		private Long threadId;
		
		private Long createTime;

		public LockInfo() {
			super();
		}

		public LockInfo(Long threadId, Long createTime) {
			super();
			this.threadId = threadId;
			this.createTime = createTime;
		}

		public Long getThreadId() {
			return threadId;
		}

		public void setThreadId(Long threadId) {
			this.threadId = threadId;
		}

		public Long getCreateTime() {
			return createTime;
		}

		public void setCreateTime(Long createTime) {
			this.createTime = createTime;
		}
		
		/**
		 * 是否过期
		 * @return
		 */
		public boolean isExpire(){
			return System.currentTimeMillis()-this.createTime>MAX_LOCK_MILLS;
		}
	}

}
