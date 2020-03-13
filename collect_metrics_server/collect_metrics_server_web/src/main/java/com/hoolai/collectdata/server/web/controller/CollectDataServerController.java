package com.hoolai.collectdata.server.web.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.hoolai.bi.collectdata.server.service.uploadfile.UploadFileService;
import com.hoolai.bi.report.util.KV;
import com.jian.tools.util.JSONUtils;

@Controller
public class CollectDataServerController extends AbstractPanelController{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CollectDataServerController.class.getSimpleName());
	
	@Autowired
	private UploadFileService uploadFileService;
	
	@RequestMapping(value = {"/collect"}, method = {RequestMethod.POST })
	public void collect(MultipartHttpServletRequest request,HttpServletResponse response,Model model){
		Map<String,Object> ret=new HashMap<String, Object>();
		List<KV<String,Boolean>> resultList=new ArrayList<KV<String,Boolean>>();
		List<KV<String,KV<File,Boolean>>> succProcessList=new ArrayList<KV<String,KV<File,Boolean>>>();
		try {
			List<MultipartFile> uploadFiles=request.getFiles("uploadFiles");
			if (!uploadFiles.isEmpty()) {
				for(MultipartFile multipartFile:uploadFiles){
					KV<File, Boolean> uploadRes=this.uploadFileService.processUploadFile(multipartFile);
					if(uploadRes==null){
						resultList.add(new KV<String, Boolean>(multipartFile.getOriginalFilename(), false));
					}else{
						resultList.add(new KV<String, Boolean>(multipartFile.getOriginalFilename(), uploadRes.getV()));
						succProcessList.add(new KV<String,KV<File,Boolean>>(multipartFile.getOriginalFilename(),uploadRes));
					}
				}
			}
			ret.put("resultList", resultList);
			ret.put("status", "succ");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			ret.put("status", "err");
			ret.put("msg", e.getMessage());
		}
		
		try {
			String jsonResult=JSONUtils.toJSON(ret);
			response.getOutputStream().write(jsonResult.getBytes("UTF-8"));
			response.getOutputStream().flush();
			if(!succProcessList.isEmpty()){
				for (KV<String, KV<File, Boolean>> succProer : succProcessList) {
					KV<File, Boolean> kv=succProer.getV();
					if(kv.getV().equals(true)){
						this.uploadFileService.renameSendSuccFile(kv.getK());
					}else{
						this.uploadFileService.clearSendFailFile(kv.getK());
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			if(!succProcessList.isEmpty()){
				for (KV<String, KV<File, Boolean>> succProer : succProcessList) {
					 KV<File, Boolean> kv=succProer.getV();
					 this.uploadFileService.clearSendFailFile(kv.getK());
				}
			}
		}
		
		
	}
	
}

