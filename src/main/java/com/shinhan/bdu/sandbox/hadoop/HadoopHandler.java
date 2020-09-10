package com.shinhan.bdu.sandbox.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;

import com.shinhan.bdu.sandbox.util.MetaReadUtil;
public class HadoopHandler {
	
	
    Configuration hdfsConfig = null;
    Path tirPath = null;
    Map<String, String> config = null;
	
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
			return fs.getQuotaUsage(hdfsDir);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
    }
	

}
