package com.hoolai.bi.collectdata.server.service.uploadfile;

import java.io.File;

import org.springframework.web.multipart.MultipartFile;

import com.hoolai.bi.report.util.KV;

public interface UploadFileService {
	
	public KV<File, Boolean> processUploadFile(MultipartFile multipartFile);
	
	public void renameSendSuccFile(File succFile);
	
	public void clearSendFailFile(File failFile);

}
