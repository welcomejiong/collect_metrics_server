package com.hoolai.bi.collectdata.server.service.mergefile;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hoolai.bi.report.job.SystemThreadPoolExecutor;

@Service
public class MergeFileServiceImpl implements MergeFileService {
	
	private static final int MAX_CORE_POOL_SIZE=16;
	
	@Autowired
	private CategoryDataDirectoryChooser categoryDataDirectoryChooser;

	public MergeFileServiceImpl() {
		super();
	}

	@Override
	public void merge() {
		try {
			List<File> files=this.categoryDataDirectoryChooser.find();
			if(files.size()<1){
				return ;
			}
			int corePoolSize=files.size()>MAX_CORE_POOL_SIZE?MAX_CORE_POOL_SIZE:files.size();
			SystemThreadPoolExecutor threadPoolExecutor=new SystemThreadPoolExecutor("merge-file", corePoolSize);
			for (File file : files) {
				CategoryMergeFileThread categoryMergeFileThread=new CategoryMergeFileThread(file);
				threadPoolExecutor.submit(categoryMergeFileThread);
			}
			threadPoolExecutor.getExecutor().shutdown();
			threadPoolExecutor.getExecutor().awaitTermination(2, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	

}
