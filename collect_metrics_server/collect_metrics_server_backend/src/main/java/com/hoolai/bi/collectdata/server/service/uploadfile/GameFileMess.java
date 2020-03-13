package com.hoolai.bi.collectdata.server.service.uploadfile;

public class GameFileMess{
	
	private final String fileName;
	
	// 文件前缀
	private final String filePrefix;
	
	// 文件后缀
	private final String fileSubfix;
	
	private boolean isMatch;
	
	private String category;
	
	private Long gameid;
	
	private String ds;
	
	public GameFileMess(String fileName){
		this.fileName=fileName;
		this.filePrefix=this.fileName.substring(0,this.fileName.indexOf("."));
		this.fileSubfix=this.fileName.substring(this.fileName.indexOf(".")+1);
		this.init();
	}
	
	private void init(){
		try {
			int firstSplitIdx=this.fileName.indexOf("_");
			int lastSplitIdx=this.fileName.lastIndexOf("_");
			if(firstSplitIdx<0||firstSplitIdx==lastSplitIdx){
				this.isMatch=false;
				return ;
			}
			
			this.isMatch=true;
			
			String category=this.fileName.substring(0, firstSplitIdx);
			String event=this.fileName.substring(0, lastSplitIdx);
			
			this.category=category;
			String[] tmpArr=event.split("_");
			this.gameid=Long.parseLong(tmpArr[1]);
			this.ds=tmpArr[2];
		} catch (Exception e) {
			e.printStackTrace();
			this.isMatch=false;
		}
	}

	public String getFileName() {
		return fileName;
	}

	public String getFilePrefix() {
		return filePrefix;
	}

	public String getFileSubfix() {
		return fileSubfix;
	}

	public boolean isMatch() {
		return isMatch;
	}

	public void setMatch(boolean isMatch) {
		this.isMatch = isMatch;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public Long getGameid() {
		return gameid;
	}

	public void setGameid(Long gameid) {
		this.gameid = gameid;
	}

	public String getDs() {
		return ds;
	}

	public void setDs(String ds) {
		this.ds = ds;
	}
	
}