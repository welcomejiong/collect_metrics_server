package com.hoolai.bi.collectdata.server.job.merge;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.hoolai.bi.collectdata.server.job.AbstractExecuteJob;
import com.hoolai.bi.collectdata.server.service.mergefile.MergeFileService;

public class MergeFileJob extends AbstractExecuteJob {
	
	@Autowired
	private MergeFileService mergeFileService;

	@Override
	public Map<String, Object> executeJob() throws Exception {
		try {
			if(!Constant.IS_MERGING_SERVER){
				return Collections.emptyMap();
			}
			this.mergeFileService.merge();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Collections.emptyMap();
	}

}
