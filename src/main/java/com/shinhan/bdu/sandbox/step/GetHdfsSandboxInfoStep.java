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
 * @desc HDFS directory information을 얻어내는 step
 * @dependency CmHandler (Cloudera manager API wrapper), Config DATA
 *
 */
public class GetHdfsSandboxInfoStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxInfoStep.class);
	private ExceptionHandler eh = new ExceptionHandler();

	private Map<String, String> makeHdfsParam(String op, String name) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("op", op);
		params.put("user.name", name);
		return params;
	}

	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		List<Map<String, Map<String, String>>> infoMaps = new ArrayList<Map<String, Map<String, String>>>();
		config = MetaReadUtil.getInstance().readHdfsConfig();

		CollectionUtil.loggingPrintMap("hdfs config", config, logger);

		HttpClient hc = new HttpClient();
		// 1. GET Sandbox List
		String sandboxListUrl = config.get("url.sandbox.list");
		Map<String, String> sandboxListParams = makeHdfsParam(config.get("op.sandbox.list")
				                                            , config.get("user.sandbox.list"));
		String sandJsontext = hc.get(sandboxListUrl, null, sandboxListParams);
		
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
			String tableListUrl = String.format(config.get("url.table.list"), sandBoxName);
			Map<String, String> tableListParams = makeHdfsParam(config.get("op.table.list")
					                                          , config.get("user.table.list"));
			String tableJsontext = hc.get(tableListUrl, null, tableListParams);
			List<Map<String, String>> tables = (List<Map<String, String>>) ((Map<String, Object>) 
														JsonUtil.getMapFeomJsonString(tableJsontext)
																.get("FileStatuses"))
															    .get("FileStatus");
			
			if (tables == null) {
				logger.error(tableListUrl);
			}
			if (tables.size() < 1) {
				logger.warn(tableListUrl + " ::: " + tableListParams);
			}
			
			for(Map<String, String> table : tables) {
				String tableName = table.get("pathSuffix");
				
				// 3. GET Table Info (use size, system size)
				String tableInfoUrl = String.format(config.get("url.table.info"), sandBoxName, tableName);
				Map<String, String> tableInfoParams = makeHdfsParam(config.get("op.table.info")
						                                          , config.get("user.table.info"));
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
				
				Map<String, Map<String, String>> rowMap = new HashMap<String, Map<String, String>>();
				String key = sandBoxName.replace(".db", "") + StaticValues.KEY_OFFSET + tableName;
				Map<String, String> rowContents = new HashMap<String, String>();
				rowContents.put("sizeUsed", sizeUsed);
				rowContents.put("spaceConsumed", sizeDisk);
				rowMap.put(key, rowContents);
				infoMaps.add(rowMap);
				
			}
		} 
		logger.info("***  hdfs info : get " + infoMaps.size() + " item's data");
		return post(input, infoMaps);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}

}
