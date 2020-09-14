package com.shinhan.bdu.sandbox.step.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.hadoop.WebHdfsHandler;
import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.step.abstact.GetHdfsDataStepImpl;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
import com.shinhan.bdu.sandbox.util.RowDataParser;
/**
 * 
 * @desc HDFS directory(root) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsRootDirInfoStep extends GetHdfsDataStepImpl {
	private final Logger logger = LoggerFactory.getLogger(GetHdfsRootDirInfoStep.class);
	
	@Override
	public Object logic(List<Map> input) throws StepException {
		
		Map<String, Map<String, String>> infoMaps = new HashMap<String, Map<String, String>>();
		WebHdfsHandler wh = new WebHdfsHandler();
		RowDataParser rp = new RowDataParser();
		// 1-1. GET HDFS root Dir Name List
		List<Map<String, String>> rootDirs = wh.getListStatus( config.get("url.root.dir.list")
													         , config.get("op.root.dir.list")
													         , config.get("user.root.dir.list"));
		// 1-2. GET HDFS root Dir information
		for(Map<String, String> dir : rootDirs) {
			String dirName = dir.get("pathSuffix");
			Map<Map, Object> infoMap = wh.getContSmry( String.format(config.get("url.root.dir.info"), dirName)
												                   , config.get("op.root.dir.info")
												                   , config.get("user.root.dir.info"));
			infoMaps.put(dirName.replace(".db", ""), rp.genInfoStepRowData(infoMap));
		} 
		
		logger.info(String.format("***  hdfs info (Root dir) : get %s item's data", infoMaps.size()));
		return infoMaps;
	}

}
