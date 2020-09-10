package com.shinhan.bdu.sandbox.step;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
/**
 * 
 * @desc HDFS directory (biz dir, file) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsBizAndFileInfoStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;
	private final Logger logger = LoggerFactory.getLogger(GetHdfsBizAndFileInfoStep.class);
	private ExceptionHandler eh = new ExceptionHandler();

	private Map<String, String> makeHdfsParam(String op, String name) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("op", op);
		params.put("user.name", name);
		return params;
	}

	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		Map<String, Map<String, String>> infoMaps = new HashMap<String, Map<String, String>>();
		config = MetaReadUtil.getInstance().readHdfsConfig();

		CollectionUtil.loggingPrintMap("hdfs config", config, logger);

		HttpClient hc = new HttpClient();
		// 1-1. GET Biz Database Name List
		String bizDbListUrl = config.get("url.biz.list");
		Map<String, String> bizDbListParams = makeHdfsParam(config.get("op.biz.list")
				                                          , config.get("user.biz.list"));
		String sandJsontext = hc.get(bizDbListUrl, null, bizDbListParams);
		
		List<Map<String, String>> bizDbs = (List<Map<String, String>>) ((Map<String, Object>) 
													JsonUtil.getMapFeomJsonString(sandJsontext)
															.get("FileStatuses"))
														    .get("FileStatus");
		if (bizDbs == null) {
			logger.error(bizDbListUrl);
		}
		if (bizDbs.size() < 1) {
			logger.warn(bizDbListUrl + " ::: " + bizDbListParams);
		}
		
		// 1-2. GET Biz Database Info 
		for(Map<String, String> bizDb : bizDbs) {
			String dbName = bizDb.get("pathSuffix");
			String tableInfoUrl = String.format(config.get("url.biz.info"), dbName);
			Map<String, String> tableInfoParams = makeHdfsParam(config.get("op.biz.info")
					                                          , config.get("user.biz.info"));
			String infoText = hc.get(tableInfoUrl, null, tableInfoParams);
			Map<Map, Object> infoMap =((Map<Map, Object>) JsonUtil.getMapFeomJsonString(infoText)
                    											  .get("ContentSummary"));
			
			if (infoMap == null) {
				logger.error(tableInfoUrl);
			}
			if (infoMap.size() < 1) {
				logger.warn(tableInfoUrl + " ::: " + tableInfoParams);
			}
			
			String sizeUsed = ""+infoMap.get("length");
			String sizeDisk = ""+infoMap.get("spaceConsumed");
			
			Map<String, String> rowContents = new HashMap<String, String>();
			rowContents.put("sizeUsed", sizeUsed);
			rowContents.put("spaceConsumed", sizeDisk);
			infoMaps.put(dbName.replace(".db", ""), rowContents);
			
		} 
		
		// 2. GET other File info
		String othersFileInfoUrl = config.get("url.other.file.info");
		Map<String, String> otherFileInfoParams = makeHdfsParam(config.get("op.other.file.info")
				                                              , config.get("user.other.file.info"));
		
		String othersFileinfoText = hc.get(othersFileInfoUrl, null, otherFileInfoParams);
		Map<Map, Object> otherInfoMap =((Map<Map, Object>) JsonUtil.getMapFeomJsonString(othersFileinfoText)
                											  	   .get("ContentSummary"));
		if (otherInfoMap == null) {
			logger.error(othersFileInfoUrl);
		}
		if (otherInfoMap.size() < 1) {
			logger.warn(othersFileInfoUrl + " ::: " + otherFileInfoParams);
		}
		
		String otehrSizeUsed = ""+otherInfoMap.get("length");
		String otherSizeDisk = ""+otherInfoMap.get("spaceConsumed");
		
		Map<String, String> otherRowContents = new HashMap<String, String>();
		otherRowContents.put("sizeUsed", otehrSizeUsed);
		otherRowContents.put("spaceConsumed", otherSizeDisk);
		infoMaps.put("otherFileArea", otherRowContents);
		
		
		// 3. GET Sandbox File info
		String sandboxFileInfoUrl = config.get("url.sandbox.file.info");
		Map<String, String> sandboxFileInfoParams = makeHdfsParam(config.get("op.sandbox.file.info")
				                                          	    , config.get("user.sandbox.file.info"));
		
		String sandboxFileinfoText = hc.get(sandboxFileInfoUrl, null, sandboxFileInfoParams);
		Map<Map, Object> sandboxInfoMap =((Map<Map, Object>) JsonUtil.getMapFeomJsonString(sandboxFileinfoText)
                											         .get("ContentSummary"));
		if (sandboxInfoMap == null) {
			logger.error(sandboxFileInfoUrl);
		}
		if (sandboxInfoMap.size() < 1) {
			logger.warn(othersFileInfoUrl + " ::: " + otherFileInfoParams);
		}
		
		String sandboxSizeUsed = ""+sandboxInfoMap.get("length");
		String sandboxSizeDisk = ""+sandboxInfoMap.get("spaceConsumed");
		
		Map<String, String> sandboxRowContents = new HashMap<String, String>();
		sandboxRowContents.put("sizeUsed", sandboxSizeUsed);
		sandboxRowContents.put("spaceConsumed", sandboxSizeDisk);
		infoMaps.put("sandboxFileArea", sandboxRowContents);
		
		logger.info(String.format("***  hdfs info (Biz and File Area) : get %s item's data", infoMaps.size()));
		return post(input, infoMaps);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}
	
	
	

}
