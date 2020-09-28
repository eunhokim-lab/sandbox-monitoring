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
import com.shinhan.bdu.sandbox.step.prd.GetHdfsDataStepImpl;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
import com.shinhan.bdu.sandbox.util.RowDataParser;
import com.shinhan.bdu.sandbox.util.StaticValues;
/**
 * 
 * @desc HDFS directory (sandbox file quota) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsSandboxFileInfoAndQuotaStep extends GetHdfsDataStepImpl {
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxFileInfoAndQuotaStep.class);

	@Override
	public Object logic(List<Map> input) throws StepException {
		
		Map<String, Map<String, String>> infoMaps = new HashMap<String, Map<String, String>>();
		WebHdfsHandler wh = new WebHdfsHandler();
		RowDataParser rp = new RowDataParser();
		
		// 1. GET Sandbox List
		List<Map<String, String>> sandboxes = wh.getListStatus( config.get("url.sandbox.file.list")
											                  , config.get("op.sandbox.file.list")
											                  , config.get("user.sandbox.file.list"));
		// 2. GET Table List
		for(Map<String, String> sandbox : sandboxes) {
			String sandBoxName = sandbox.get("pathSuffix");
			Map<String, Object> infoQuotaMap = wh.getContSmry( String.format(config.get("url.sandbox.file.detail.info"), sandBoxName)
										                                   , config.get("op.sandbox.file.detail.info")
										                                   , config.get("user.sandbox.file.detail.info"));
			infoMaps.put(sandBoxName.replace(".db", ""), rp.genQuotaInfoStepRowData(infoQuotaMap));
		} 
		logger.info("***  Sandbox File Info : get " + infoMaps.size() + " item's data");
		return infoMaps;
	}
}
