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
 * @desc HDFS directory (db영역 quota) information을 얻어내는 step
 * @dependency  webhdfs, HadoopHandler
 *
 */
public class GetHdfsSandboxDbQuotaStep extends GetHdfsDataStepImpl  {
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxDbQuotaStep.class);

	@Override
	public Object logic(List<Map> input) throws StepException {

		Map<String, Map<String, String>> infoMaps = new HashMap<String, Map<String, String>>();
		WebHdfsHandler wh = new WebHdfsHandler();
		RowDataParser rp = new RowDataParser();

		// 1. GET Sandbox List
		List<Map<String, String>> sandboxes = wh.getListStatus( config.get("url.sandbox.list")
											                  , config.get("op.sandbox.list")
											                  , config.get("user.sandbox.list"));
		// 2. GET Table Quota (name, space)
		int emptyCount = 0;
		for(Map<String, String> sandbox : sandboxes) {
			String sandBoxName = sandbox.get("pathSuffix");
			
			HadoopHandler hdh = new HadoopHandler();
			String targetPath = String.format(config.get("hdfs.sandbox.db.path"), sandBoxName);
			QuotaUsage quotaUsage = hdh.getDirQuota(targetPath);
			
			if (quotaUsage.getQuota() < 0 && 
				quotaUsage.getSpaceConsumed() < 0 && 
				quotaUsage.getSpaceQuota() < 0) {
				emptyCount ++;
			}

			infoMaps.put(sandBoxName.replace(".db", ""), rp.genQuotaInfoStepWith3rdRowData(quotaUsage));
		}
		if (emptyCount / infoMaps.size() > 0.90) {
			logger.warn("Empty Table Quota RATIO > 0.90.");
		}
		logger.info("***  Sandbox DB Quota : get " + infoMaps.size() + " item's data");
		return infoMaps;
	}

}
