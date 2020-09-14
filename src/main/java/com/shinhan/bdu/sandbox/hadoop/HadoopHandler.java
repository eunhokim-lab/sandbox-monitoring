package com.shinhan.bdu.sandbox.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
public class HadoopHandler {
	
	private final Logger logger = LoggerFactory.getLogger(HadoopHandler.class);
	
    Configuration hdfsConfig = null;
    Path tirPath = null;
    Map<String, String> config = null;
    
    private void assertInputParamsNull(Object rtn, Path hdfsDir) {
		if (rtn == null) {
			logger.error("Path{}'s is null] ", hdfsDir);
			throw new NullPointerException();
		}
	}
    
    public HadoopHandler() {
    	config = MetaReadUtil.getInstance().readHadoopConfig();
    	hdfsConfig = new Configuration();
    	hdfsConfig.addResource(new Path("file:///etc/hadoop/conf/core-site.xml")); 
    	hdfsConfig.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml")); 

    	for(String k : config.keySet()){
    		hdfsConfig.set(k, config.get(k));
    	}

    }
    
    public QuotaUsage getDirQuota(String path) {
    	
    	Path hdfsDir = new Path(path);
    	try {
			FileSystem fs = hdfsDir.getFileSystem(hdfsConfig);
			QuotaUsage quotaUsage = fs.getQuotaUsage(hdfsDir);
			assertInputParamsNull(quotaUsage, hdfsDir);
			return quotaUsage;
			
		} catch (IOException ex) {
			logger.error("{}", new ExceptionHandler().getPrintStackTrace(ex));
		}
		return null;
    }
	

}
