package com.shinhan.bdu.sandbox.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
public class JsonUtil {
	
	/**
	 * Json String을 Map으로 변환한다.
	 *
	 * @param String.
	 * @return map Map<String, Object>.
	 */
	public static Map<String, Object> getMapFeomJsonString(String jsonStr) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = null;
		try {
			map = mapper.readValue(jsonStr, new TypeReference<Map<String,Object>>(){});
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}

	
	/**
	 * Map을 json으로 변환한다.
	 *
	 * @param map Map<String, Object>.
	 * @return JSONObject.
	 */
	public static JSONObject getJsonStringFromMap(Map<String, Object> map) {
		JSONObject jsonObject = new JSONObject();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			jsonObject.put(key, value);
		}

		return jsonObject;
	}

	/**
	 * List<Map>을 jsonArray로 변환한다.
	 *
	 * @param list List<Map<String, Object>>.
	 * @return JSONArray.
	 */
	public static JSONArray getJsonArrayFromList(List<Map<String, Object>> list) {
		JSONArray jsonArray = new JSONArray();
		for (Map<String, Object> map : list) {
			jsonArray.add(getJsonStringFromMap(map));
		}

		return jsonArray;
	}

	/**
	 * List<Map>을 jsonString으로 변환한다.
	 *
	 * @param list List<Map<String, Object>>.
	 * @return String.
	 */
	public static String getJsonStringFromList(List<Map<String, Object>> list) {
		JSONArray jsonArray = getJsonArrayFromList(list);
		return jsonArray.toJSONString();
	}

	/**
	 * JsonObject를 Map<String, String>으로 변환한다.
	 *
	 * @param jsonObj JSONObject.
	 * @return Map<String, Object>.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getMapFromJsonObject(JSONObject jsonObj) {
		Map<String, Object> map = null;

		try {
			map = new ObjectMapper().readValue(jsonObj.toJSONString(), Map.class);

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return map;
	}

	/**
	 * JsonArray를 List<Map<String, String>>으로 변환한다.
	 *
	 * @param jsonArray JSONArray.
	 * @return List<Map<String, Object>>.
	 */
	public static List<Map<String, Object>> writeFileFromJsonList(JSONArray jsonArray) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		if (jsonArray != null) {
			int jsonSize = jsonArray.size();
			for (int i = 0; i < jsonSize; i++) {
				Map<String, Object> map = JsonUtil.getMapFromJsonObject((JSONObject) jsonArray.get(i));
				list.add(map);
			}
		}

		return list;
	}
	
	
	/**
	 * JsonArray를 List<Map<String, String>>으로 변환한다.
	 *
	 * @param jsonArray JSONArray.
	 * @return List<Map<String, Object>>.
	 */
	public static List<Map<String, Object>> getListMapFromJsonArray(JSONArray jsonArray) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		if (jsonArray != null) {
			int jsonSize = jsonArray.size();
			for (int i = 0; i < jsonSize; i++) {
				Map<String, Object> map = JsonUtil.getMapFromJsonObject((JSONObject) jsonArray.get(i));
				list.add(map);
			}
		}

		return list;
	}
	
	/**
	 * List<Map<String, String>>으로 json file을 생성한다.
	 *
	 * @param List<Map<String, Object>>.
	 * @return json file
	 */
	public static void getJsonFileFromListMap(String jsonName, List<Map<String, Object>> list) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.writeValue(new File(jsonName),list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * List<Map<String, String>>으로 json file을 생성한다.
	 *
	 * @param List<Map<String, Object>>.
	 * @return json file
	 */
	public static void getJsonFileFromListMap(File file, List<Map<String, Object>> list) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.writeValue(file,list);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
}
