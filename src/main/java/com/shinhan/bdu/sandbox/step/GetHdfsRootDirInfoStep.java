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
 * @desc HDFS directory(root) information을 얻어내는 step
 * @dependency  webhdfs
 *
 */
public class GetHdfsRootDirInfoStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;
	private final Logger logger = LoggerFactory.getLogger(GetHdfsRootDirInfoStep.class);
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
		// 1-1. GET HDFS root Dir Name List
		String rootDirListUrl = config.get("url.root.dir.list");
		Map<String, String> rootDirListParams = makeHdfsParam(config.get("op.root.dir.list")
				                                            , config.get("user.root.dir.info"));
		String rootDirJsontext = hc.get(rootDirListUrl, null, rootDirListParams);
		
		List<Map<String, String>> rootDirs = (List<Map<String, String>>) ((Map<String, Object>) 
													JsonUtil.getMapFeomJsonString(rootDirJsontext)
															.get("FileStatuses"))
														    .get("FileStatus");
		if (rootDirs == null) {
			logger.error(rootDirListUrl);
		}
		if (rootDirs.size() < 1) {
			logger.warn(rootDirListUrl + " ::: " + rootDirListParams);
		}
		
		// 1-2. GET HDFS root Dir information
		for(Map<String, String> dir : rootDirs) {
			String dirName = dir.get("pathSuffix");
			String rootDirInfoUrl = String.format(config.get("url.root.dir.info"), dirName);
			Map<String, String> rootDirInfoParams = makeHdfsParam(config.get("op.root.dir.info")
					                                            , config.get("user.root.dir.info"));
			String infoText = hc.get(rootDirInfoUrl, null, rootDirInfoParams);
			Map<Map, Object> infoMap =((Map<Map, Object>) JsonUtil.getMapFeomJsonString(infoText)
                    											  .get("ContentSummary"));
			
			if (infoMap == null) {
				logger.error(rootDirInfoUrl);
			}
			if (infoMap.size() < 1) {
				logger.warn(rootDirInfoUrl + " ::: " + rootDirInfoParams);
			}
			
			String sizeUsed = ""+infoMap.get("length");
			String sizeDisk = ""+infoMap.get("spaceConsumed");
			
			Map<String, String> rowContents = new HashMap<String, String>();
			rowContents.put("sizeUsed", sizeUsed);
			rowContents.put("spaceConsumed", sizeDisk);
			infoMaps.put(dirName.replace(".db", ""), rowContents);
		} 
		
		logger.info(String.format("***  hdfs info (Root dir) : get %s item's data", infoMaps.size()));
		return post(input, infoMaps);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}
	
	
	

}
