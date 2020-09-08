package com.shinhan.bdu.sandbox.util;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * 
 * @desc convert point Query's change block
 *
 */

public class QueryConverter {
	
	private static final String MON_1_AGO = "to_date(add_months(now(), -1))";
	private static final String MON_2_AGO = "to_date(add_months(now(), -2))";
	private static final String MON_3_AGO = "to_date(add_months(now(), -3))";
	private static final String MON_4_AGO = "to_date(add_months(now(), -4))";
	private static final String MON_5_AGO = "to_date(add_months(now(), -5))";
	private static final String MON_6_AGO = "to_date(add_months(now(), -6))";
	private static final String MON_7_AGO = "to_date(add_months(now(), -7))";
	private static final String MON_8_AGO = "to_date(add_months(now(), -8))";
	private static final String MON_9_AGO = "to_date(add_months(now(), -9))";
	private static final String MON_10_AGO = "to_date(add_months(now(), -10))";
	private static final String MON_11_AGO = "to_date(add_months(now(), -11))";
	private static final String YEAR_1_AGO = "to_date(add_months(now(), -12))";
	
	private static QueryConverter instance = null;
	
	private QueryConverter() {
	}
	
	synchronized public static QueryConverter getInstance() {
		if (instance == null) {
			instance = new QueryConverter();
		}
		return instance;
	}
	
	/**
	 * @param origin : original query
	 * @param changeMap : target table select 절등 변경 part
	 * @return 변경된 query string
	 */
	public String convert(String origin, Map<String, String> changeMap) {
		if (changeMap == null )
			return origin;
		for(String k : changeMap.keySet()){
			origin = origin.replace(k, changeMap.get(k));
		}
		return origin;
	}
	
	private String changeRelTimeType(String rel) {
		if(rel == null)
			return this.YEAR_1_AGO;
		rel = rel.toLowerCase();
		if (rel.endsWith("y")) {
			return this.YEAR_1_AGO;
		}
		if (rel.endsWith("m")) {
			if (rel.startsWith("1"))
				return this.MON_1_AGO;
			else if (rel.startsWith("2"))
				return this.MON_2_AGO;
			else if (rel.startsWith("3"))
				return this.MON_3_AGO;
			else if (rel.startsWith("4"))
				return this.MON_4_AGO;
			else if (rel.startsWith("5"))
				return this.MON_5_AGO;
			else if (rel.startsWith("6"))
				return this.MON_6_AGO;
			else if (rel.startsWith("7"))
				return this.MON_7_AGO;
			else if (rel.startsWith("8"))
				return this.MON_8_AGO;
			else if (rel.startsWith("9"))
				return this.MON_9_AGO;
			else if (rel.startsWith("10"))
				return this.MON_10_AGO;
			else if (rel.startsWith("11"))
				return this.MON_11_AGO;
		}
		return this.YEAR_1_AGO;
		
	}
	
	public String convertReltime(String origin, String rel) {		
		rel = changeRelTimeType(rel);
		return origin.replace("{@start_time}", rel);
	}
	
}
