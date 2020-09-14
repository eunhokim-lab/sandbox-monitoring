package com.shinhan.bdu.sandbox.step.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.hadoop.WebHdfsHandler;
import com.shinhan.bdu.sandbox.step.prd.GetHdfsDataStepImpl;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.RowDataParser;
/**
 * 
 * @desc HDFS directory (biz dir, file) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsBizAndFileInfoStep extends GetHdfsDataStepImpl {
	
	private final Logger logger = LoggerFactory.getLogger(GetHdfsBizAndFileInfoStep.class);

	@Override
	public Object logic(List<Map> input) throws StepException {
		
		WebHdfsHandler wh = new WebHdfsHandler();
		RowDataParser rp = new RowDataParser();
		Map<String, Map<String, String>> infoMaps = new HashMap<String, Map<String, String>>();
		
		// 1-1. GET Biz Database Name List
		List<Map<String, String>> bizDbs = wh.getListStatus( config.get("url.biz.list")
				                                           , config.get("op.biz.list")
				                                           , config.get("user.biz.list"));
		// 1-2. GET Biz Database Info 
		for(Map<String, String> bizDb : bizDbs) {
			String dbName = bizDb.get("pathSuffix");
			Map<Map, Object> infoMap = wh.getContSmry( String.format(config.get("url.biz.info"), dbName)
									                 , config.get("op.biz.info")
									                 , config.get("user.biz.info"));
			infoMaps.put(dbName.replace(".db", ""), rp.genInfoStepRowData(infoMap));
		} 
		// 2. GET other File info
		Map<Map, Object> otherInfoMap = wh.getContSmry( config.get("url.other.file.info")
										              , config.get("op.other.file.info")
										              , config.get("user.other.file.info"));
		infoMaps.put("otherFileArea", rp.genInfoStepRowData(otherInfoMap));
		// 3. GET Sandbox File info
		Map<Map, Object> sandboxInfoMap = wh.getContSmry( config.get("url.sandbox.file.info")
								                 , config.get("op.sandbox.file.info")
								                 , config.get("user.sandbox.file.info"));
		infoMaps.put("sandboxFileArea", rp.genInfoStepRowData(sandboxInfoMap));
		
		logger.info(String.format("***  hdfs info (Biz and File Area) : get %s item's data", infoMaps.size()));
		return infoMaps;
	}
	
	
	

}
