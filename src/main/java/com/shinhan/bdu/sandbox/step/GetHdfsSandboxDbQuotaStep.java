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
 * @desc HDFS directory (db영역 quota) information을 얻어내는 step
 * @dependency  webhdfs, HadoopHandler
 *
 */
public class GetHdfsSandboxDbQuotaStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;
	private final Logger logger = LoggerFactory.getLogger(GetHdfsSandboxDbQuotaStep.class);
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
		String sandboxListUrl = config.get("url.sandbox.list");
		Map<String, String> sandboxListParams = makeHdfsParam(config.get("op.sandbox.list"),
				config.get("user.sandbox.list"));
		String sandJsontext = hc.get(sandboxListUrl, null, sandboxListParams);

		List<Map<String, String>> sandboxes = (List<Map<String, String>>) ((Map<String, Object>) JsonUtil
				.getMapFeomJsonString(sandJsontext).get("FileStatuses")).get("FileStatus");
		
		if (sandboxes == null) {
			logger.error(sandboxListUrl);
		}
		if (sandboxes.size() < 1) {
			logger.warn(sandboxListUrl + " ::: " + sandboxListParams);
		}
		
		// 2. GET Table Quota (name, space)
		int emptyCount = 0;
		for (Map<String, String> sandbox : sandboxes) {
			String sandBoxName = sandbox.get("pathSuffix");
			config = MetaReadUtil.getInstance().readHdfsConfig();
			HadoopHandler hdfs = new HadoopHandler();

			String targetPath = String.format(config.get("hdfs.sandbox.db.path"), sandBoxName);
			QuotaUsage quotaUsage = hdfs.getDirQuota(targetPath);
			
			if (quotaUsage == null) {
				logger.error(sandboxListUrl);
			}
			if (quotaUsage.getQuota() < 0 && 
				quotaUsage.getSpaceConsumed() < 0 && 
				quotaUsage.getSpaceQuota() < 0) {
				emptyCount ++;
			}
			
			String quota = "" + quotaUsage.getQuota();
			String spaceConsumed = "" + quotaUsage.getSpaceConsumed();
			String spaceQuota = "" + quotaUsage.getSpaceQuota();

			Map<String, Map<String, String>> rowMap = new HashMap<String, Map<String, String>>();
			Map<String, String> rowContents = new HashMap<String, String>();
			rowContents.put("quota", quota);
			rowContents.put("spaceConsumed", spaceConsumed);
			rowContents.put("spaceQuota", spaceQuota);
			infoMaps.put(sandBoxName.replace(".db", ""), rowContents);
		}
		if (emptyCount / infoMaps.size() > 0.90) {
			logger.warn("Empty Table Quota RATIO");
		}
		logger.info("***  Sandbox DB Quota : get " + infoMaps.size() + " item's data");
		return post(input, infoMaps);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data);
		input.get(0).put("status", "finish");
		input.add(input.remove(0));
		return input;
	}

}
