package com.shinhan.bdu.sandbox.http;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;


public class HttpClient {
	
	private String formatQueryParams(Map<String, String> params) {
      return params.entrySet().stream()
              .map(p -> p.getKey() + "=" + p.getValue())
              .reduce((p1, p2) -> p1 + "&" + p2)
              .map(s -> "?" + s)
              .orElse("");
    }
	
	public String get(String requestURL, Map<String, String> headers, Map<String, String> params) {
		try {
			if (params != null)
				requestURL = requestURL + formatQueryParams(params);
			
		    HttpGet httpGet = new HttpGet(requestURL);
		    
		    if (headers != null)
			    for (String key : headers.keySet()) 
			    	httpGet.setHeader(key, headers.get(key));

		    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		    CloseableHttpResponse response = httpClient.execute(httpGet);
		    
		    if (response.getStatusLine().getStatusCode() == 200) {

		        ResponseHandler<String> handler = new BasicResponseHandler();
		        String body = handler.handleResponse(response);
		        return body;
		        
		    }
		} catch(Exception e) {
		    e.printStackTrace();
		}
		return null;
	}

	private List<NameValuePair> mapToPostParam(Map<String, String> jsonMessage) {
		List<NameValuePair> postParams = new ArrayList<NameValuePair>();
		for (String key : jsonMessage.keySet()) 
			postParams.add(new BasicNameValuePair(key, jsonMessage.get(key)));
	    
	    return postParams;
	}

	public String post(String requestURL, Map<String, String> jsonMessage, Map<String, String> headers) {

		try {
			
			if (jsonMessage == null) {
				System.out.println("post body 정보가 없습니다.");
				throw new NullPointerException();
			}
			
		    HttpPost httpPost = new HttpPost(requestURL);
		    if (headers != null)
			    for (String key : headers.keySet()) 
			    	httpPost.setHeader(key, headers.get(key));

		    List<NameValuePair> postParams = mapToPostParam(jsonMessage);

		    //Post 방식인 경우 데이터를 Request body message에 전송
		    HttpEntity postEntity = new UrlEncodedFormEntity(postParams, "UTF-8");
		    httpPost.setEntity(postEntity);

		    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		    CloseableHttpResponse response = httpClient.execute(httpPost);

		    if (response.getStatusLine().getStatusCode() == 200) {

		        ResponseHandler<String> handler = new BasicResponseHandler();
		        String body = handler.handleResponse(response);
		        return body;
		    }
		} catch(Exception e) {
		    e.printStackTrace();
		}
		return null;
	}
	
	
	private void debugPrintSysout(String body) {
		System.out.println("--------------------------- HTTP  CLINE ---------------------------");
		System.out.println("------------------------------ DEBUG ------------------------------");
		System.out.println(body);
	}
	
}
