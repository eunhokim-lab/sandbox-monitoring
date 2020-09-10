package com.shinhan.bdu.sandbox.step;

import java.util.ArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.SizeLimitExceededException;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.step.Step.StepException;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
/**
 * 
 * @desc HDFS directory info와 impala table 사용량을 summary & 구조/정규화
 *
 */
public class SummaryInfoStep implements Step<List<Map>, List<Map>> {
	private final Logger logger = LoggerFactory.getLogger(SummaryInfoStep.class);
	
	private List<Map<String, Map<String, String>>> getFinishedStepOuput(List<Map> input){
		List<Map<String, Map<String, String>>> mergedOutput = new ArrayList<Map<String, Map<String, String>>>();
		for (Map<String, Object> step : input) {
			if(step.keySet().containsAll(new ArrayList<String>(Arrays.asList("status", "output"))) &&
			   step.get("status").equals("finish") && step.get("output") != null) {
				mergedOutput.addAll((Collection<? extends Map<String, Map<String, String>>>) step.get("output"));
			}
		}
		return mergedOutput;
	}
	
	private List<String> getTargetKeys(List<Map> input){
		boolean first = true;
		List<String> tagetKeys = null;
		for (Map<String, Object> step : input) {
			if(step.keySet().containsAll(new ArrayList<String>(Arrays.asList("status", "output"))) &&
			   step.get("status").equals("finish") && step.get("output") != null) {
				if (first) {
					tagetKeys = CollectionUtil.getKeysWithMapList((List<Map>)step.get("output"));
					first = false;
					return tagetKeys;
				} else {
					break;
				}
			}
		}
		
		return tagetKeys;
	}
	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		Map<String, Map<String, String>> summary = CollectionUtil.mergeMapsValueLeftJoin(getFinishedStepOuput(input), 
				                                                                         getTargetKeys(input));
		logger.info("*** smry step : summurize table useage & hdfs capa " + summary.size() + " items.");
		return post(input, summary);
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data);
		input.get(0).put("status", "finish");
		input.add(input.remove(0));
		return input;
	}

}
