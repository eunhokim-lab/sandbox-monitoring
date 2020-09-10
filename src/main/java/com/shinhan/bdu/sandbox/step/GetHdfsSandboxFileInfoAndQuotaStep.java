package com.shinhan.bdu.sandbox.step;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.QuotaUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.hadoop.HadoopHandler;
import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
import com.shinhan.bdu.sandbox.util.StaticValues;
/**
 * 
 * @desc HDFS directory (sandbox file quota) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsSandboxFileInfoAndQuotaStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxFileInfoAndQuotaStep.class);
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
		// 1. GET Sandbox List
		String sandboxListUrl = config.get("url.sandbox.file.list");
		Map<String, String> sandboxListParams = makeHdfsParam(config.get("op.sandbox.file.list")
				                                            , config.get("user.sandbox.file.list"));
		String sandJsontext = hc.get(sandboxListUrl, null, sandboxListParams);
		@SuppressWarnings("unchecked")
		List<Map<String, String>> sandboxes = (List<Map<String, String>>) ((Map<String, Object>) 
													JsonUtil.getMapFeomJsonString(sandJsontext)
															.get("FileStatuses"))
														    .get("FileStatus");
		if (sandboxes == null) {
			logger.error(sandboxListUrl);
		}
		if (sandboxes.size() < 1) {
			logger.warn(sandboxListUrl + " ::: " + sandboxListParams);
		}
		
		// 2. GET Table List
		for(Map<String, String> sandbox : sandboxes) {
			String sandBoxName = sandbox.get("pathSuffix");
			
			
			String fileInfoQuotaUrl = String.format(config.get("url.sandbox.file.detail.info"), sandBoxName);
			Map<String, String> fileInfoQuotaParams = makeHdfsParam(config.get("op.sandbox.file.detail.info")
					                                              , config.get("user.sandbox.file.detail.info"));
			
			String fileInfoJsontext = hc.get(fileInfoQuotaUrl, null, fileInfoQuotaParams);
			Map<Map, Object> infoQuotaMap =((Map<Map, Object>) JsonUtil.getMapFeomJsonString(fileInfoJsontext)
						                                               .get("ContentSummary"));
				
			String sizeUsed = ""+infoQuotaMap.get("length");
			String sizeDisk = ""+infoQuotaMap.get("spaceConsumed");
			String spaceQuota = ""+infoQuotaMap.get("spaceQuota");
			
			Map<String, Map<String, String>> rowMap = new HashMap<String, Map<String, String>>();
			String key = sandBoxName.replace(".db", "");
			Map<String, String> rowContents = new HashMap<String, String>();
			rowContents.put("sizeUsed", sizeUsed);
			rowContents.put("spaceConsumed", sizeDisk);
			rowContents.put("spaceQuota", spaceQuota);
			infoMaps.put(key, rowContents);
				
		} 
		logger.info("***  Sandbox File Info : get " + infoMaps.size() + " item's data");
		return post(input, infoMaps);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}

}
