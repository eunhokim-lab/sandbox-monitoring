package com.shinhan.bdu.sandbox.step.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.QuotaUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.hadoop.HadoopHandler;
import com.shinhan.bdu.sandbox.hadoop.WebHdfsHandler;
import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.step.abstact.GetHdfsDataStepImpl;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
import com.shinhan.bdu.sandbox.util.RowDataParser;
import com.shinhan.bdu.sandbox.util.StaticValues;
/**
 * 
 * @desc HDFS directory (sandbox db영역) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsSandboxDbInfoStep extends GetHdfsDataStepImpl {
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxDbInfoStep.class);

	@Override
	public Object logic(List<Map> input) throws StepException {
		
		List<Map<String, Map<String, String>>> infoMaps = new ArrayList<Map<String, Map<String, String>>>();
		WebHdfsHandler wh = new WebHdfsHandler();
		RowDataParser rp = new RowDataParser();
		// 1. Get Sandbox List
		List<Map<String, String>> sandboxes = wh.getListStatus( config.get("url.sandbox.list")
											                  , config.get("op.sandbox.list")
											                  , config.get("user.sandbox.list"));
		// 2. GET Table List
		for(Map<String, String> sandbox : sandboxes) {
			String sandBoxName = sandbox.get("pathSuffix");
			List<Map<String, String>> tables = wh.getListStatus( String.format(config.get("url.table.list"), sandBoxName)
											                   , config.get("op.table.list")
											                   , config.get("user.table.list"));
			// 3. GET Table Info (use size, system size)
			for(Map<String, String> table : tables) {
				String tableName = table.get("pathSuffix");
				Map<Map, Object> infoMap = wh.getContSmry( String.format(config.get("url.table.info"), sandBoxName, tableName)
														               , config.get("op.table.info")
														               , config.get("user.table.info"));
				Map<String, Map<String, String>> rowMap = new HashMap<String, Map<String, String>>();
				String key = sandBoxName.replace(".db", "") + StaticValues.KEY_OFFSET + tableName;
				rowMap.put(key, rp.genInfoStepRowData(infoMap));
				infoMaps.add(rowMap);
			}
		} 
		logger.info("***  Sandbox DB info : get " + infoMaps.size() + " item's data");
		return infoMaps;
	}
}
