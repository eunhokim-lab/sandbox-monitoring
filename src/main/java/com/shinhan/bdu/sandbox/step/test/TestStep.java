package com.shinhan.bdu.sandbox.step.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.shinhan.bdu.sandbox.hadoop.HadoopHandler;
import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.step.prd.Step;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;

/**
 * 
 * @desc HDFS directory Quota값을 얻어내는 step
 * @dependency HdfsHandler, Config DATA
 *
 */
public class TestStep implements Step<List<Map>, List<Map>> {
	private Map<String, String> config;

	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		config = MetaReadUtil.getInstance().readHdfsConfig();
		
		HadoopHandler hdfs = new HadoopHandler();
		System.out.println(hdfs.getDirQuota(config.get("hdfs.test.host")));
		
		return null;
	}

}
