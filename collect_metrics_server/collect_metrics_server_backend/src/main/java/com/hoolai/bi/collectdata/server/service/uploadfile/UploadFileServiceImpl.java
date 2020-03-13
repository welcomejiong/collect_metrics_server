package com.hoolai.bi.collectdata.server.service.uploadfile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.hoolai.bi.compress.CompressType;
import com.hoolai.bi.report.util.KV;

@Service
public class UploadFileServiceImpl implements UploadFileService {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(UploadFileServiceImpl.class.getSimpleName());
	
	private final int bufferLen=1024*2;
	
	private final AtomicInteger fileIndex=new AtomicInteger(0);

	@Override
	public KV<File, Boolean> processUploadFile(MultipartFile multipartFile) {
		if(multipartFile==null){
			return null;
		}
		String name=multipartFile.getOriginalFilename();
		GameFileMess gameFileMess=new GameFileMess(name);
		File targetFile=this.parsePersistFile(gameFileMess);
		if(targetFile==null){
			return null;
		}
		try {
			InputStream inputstream=null;
			if(Constant.compressType.equals(CompressType.ZIP)){
				ZipArchiveInputStream zipInputstream=new ZipArchiveInputStream(multipartFile.getInputStream());
				inputstream=zipInputstream;
				ZipArchiveEntry entry = null;  
				while ((entry = zipInputstream.getNextZipEntry()) != null) {  
				    if (entry.isDirectory()) {  
				        // 不做处理
				    } else {  
				        OutputStream os = null;  
				        try {  
				            os = new BufferedOutputStream(new FileOutputStream(targetFile), bufferLen);  
				            IOUtils.copy(zipInputstream, os);  
				        } finally {  
				            IOUtils.closeQuietly(os);  
				        }  
				    }  
				}  
			}else{
				inputstream=new GzipCompressorInputStream(multipartFile.getInputStream());
				OutputStream os = new BufferedOutputStream(new FileOutputStream(targetFile), bufferLen);
				IOUtils.copy(inputstream, os);
				IOUtils.closeQuietly(os);  
			}
			IOUtils.closeQuietly(inputstream);
			LOGGER.info("process OriginalFilename:"+name+" for serverFileName:"+targetFile.getAbsolutePath()+" succ");
			return new KV<File, Boolean>(targetFile,true);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			return new KV<File, Boolean>(targetFile,false);
		}
	}
	
	public void renameSendSuccFile(File succFile){
		try {
			if(succFile==null){
				return ;
			}
			String currPath=succFile.getPath();
			String fileNameSubfix=currPath.substring(0, currPath.lastIndexOf("."));
			File dest = new File(fileNameSubfix + ".rowdata");
			boolean renamed = succFile.renameTo(dest);
		    if (renamed) {
		    	LOGGER.info("successfully rename send succ file  for path:"+succFile.getPath() +" to "+dest.getPath());
		    }
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	/**
	 * 如果发送失败，则删除已经持久化到硬盘的数据
	 * @param failFile
	 */
	public void clearSendFailFile(File failFile){
		try {
			if(failFile==null){
				return ;
			}
			failFile.delete();
			LOGGER.info("clear send fail file  for path:"+failFile.getPath());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	private File parsePersistFile(GameFileMess gameFileMess){
		if(gameFileMess==null||!gameFileMess.isMatch()){
			return null;
		}
		try {
			StringBuilder targetFilePath=new StringBuilder(Constant.UPLOAD_PATH);
			// 指标目录
			targetFilePath.append(File.separator).append(gameFileMess.getCategory());
			// 指标、snid、gameid、ds目录
			targetFilePath.append(File.separator).append(gameFileMess.getGameid());
			targetFilePath.append(File.separator).append(gameFileMess.getDs());
			
			File targetFileDir=new File(targetFilePath.toString());
			if(!targetFileDir.exists()){
				targetFileDir.mkdirs();
			}
			// 指标、snid、gameid、ds 文件名
			File targetFile=new File(targetFilePath.toString()+File.separator+gameFileMess.getFilePrefix()+"_"+System.currentTimeMillis()+"-"+fileIndex.incrementAndGet()+".buffer");
			if(!targetFile.exists()){
				targetFile.createNewFile();
				LOGGER.info("createNewFile for path:"+targetFile.getPath());
			}else{
				LOGGER.info("instance output for path:"+targetFile.getPath());
			}
			return targetFile;
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
		}
		return null;
	}

}
