package com.shinhan.bdu.sandbox.hadoop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.step.prd.GetHdfsDataStepImpl;
import com.shinhan.bdu.sandbox.util.JsonUtil;
public class WebHdfsHandler {
	
	private final Logger logger = LoggerFactory.getLogger(WebHdfsHandler.class);
	
	private Map<String, String> makeHdfsParam(String op, String name) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("op", op);
		params.put("user.name", name);
		return params;
	}
	
	private void assertWebHdfsReturnData(Object data, int size, String url, Map<String, String> params) {
		if (data == null) {
			logger.error("[ webHdfs::get HDFS Info ] : Data is null " + url);
			throw new NullPointerException();
		}
		if (size < 1)
			logger.warn("[ webHdfs::get HDFS Info ] : Data size is 0 " + url + " ::: " + params);
	}
	
	private void assertInput(String ... params) {
		for (String param : params){
			if (param == null) {
				logger.error("[ webHdfs::get HDFS Info ] : Parmeter {} is null ", param);
				throw new NullPointerException();
			}
		}
	}
		
	
	public List<Map<String, String>> getListStatus(String url, String op, String user) {
		assertInput(url, op, user);
		logger.debug("[ webHdfs::getListStatus ] : {}, {}, {}", url, op, user);
		HttpClient hc = new HttpClient();
		Map<String, String> listParams = makeHdfsParam(op, user);
		String sandJsontext = hc.get(url, null, listParams);
		List<Map<String, String>> lists = (List<Map<String, String>>) ((Map<String, Object>) 
													JsonUtil.getMapFeomJsonString(sandJsontext)
															.get("FileStatuses"))
														    .get("FileStatus");
		assertWebHdfsReturnData(lists, lists.size(), url, listParams);
		return lists;
	}
	
	public Map<String, Object> getContSmry(String url, String op, String user) {
		assertInput(url, op, user);
		logger.debug("[ webHdfs::getListStatus ] : {}, {}, {}", url, op, user);
		HttpClient hc = new HttpClient();
		Map<String, String> infoParams = makeHdfsParam(op, user);
		String infoText = hc.get(url, null, infoParams);
		Map<String, Object> infos =((Map<String, Object>) JsonUtil.getMapFeomJsonString(infoText)
                											.get("ContentSummary"));
		
		assertWebHdfsReturnData(infos, infos.size(), url, infoParams);
		return infos;
	}
	
}
