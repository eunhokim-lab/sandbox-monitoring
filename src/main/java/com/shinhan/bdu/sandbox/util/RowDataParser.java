package com.shinhan.bdu.sandbox.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.QuotaUsage;

public class RowDataParser {
	
	public Map<String, String> genInfoStepRowData(Map<Map, Object> dataMap) {
		Map<String, String> rowContents = new HashMap<String, String>();
		rowContents.put("sizeUsed", ""+dataMap.get("length"));
		rowContents.put("spaceConsumed", ""+dataMap.get("length"));
		return rowContents;
	}
	
	public Map<String, String> genQuotaInfoStepRowData(Map<Map, Object> dataMap) {
		Map<String, String> rowContents = new HashMap<String, String>();
		rowContents.put("sizeUsed", ""+dataMap.get("length"));
		rowContents.put("spaceConsumed", ""+dataMap.get("length"));
		rowContents.put("spaceQuota", ""+dataMap.get("spaceQuota"));
		return rowContents;
	}
	public Map<String, String> genQuotaInfoStepWith3rdRowData(QuotaUsage quotaUsage) {
		Map<String, String> rowContents = new HashMap<String, String>();
		rowContents.put("quota", ""+quotaUsage.getQuota());
		rowContents.put("spaceConsumed", ""+quotaUsage.getSpaceConsumed());
		rowContents.put("spaceQuota", ""+quotaUsage.getSpaceQuota());
		return rowContents;
	}
	
}
