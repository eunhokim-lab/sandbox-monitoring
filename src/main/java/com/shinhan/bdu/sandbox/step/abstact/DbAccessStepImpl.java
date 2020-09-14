package com.shinhan.bdu.sandbox.step.abstact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.step.abstact.Step.StepException;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;

public abstract class DbAccessStepImpl extends StepImp<List<Map>, List<Map>>{
	
	private final Logger logger = LoggerFactory.getLogger(DbAccessStepImpl.class);
	protected Map<String, String> config = null;
	
	protected abstract String getQuery(List<Map> input);
	
	@Override
	protected void before(List<Map> input) throws StepException {
		this.config = MetaReadUtil.getInstance().readHdfsConfig();
		CollectionUtil.loggingPrintMap("hdfs config", config, logger);
	}
	
	
	@Override
	protected List<Map> post(List<Map> input, Object data) throws StepException {
		if (data == null)
			if(input.size() == 1) throw new NullPointerException();
			else data = input.get(input.size()-1).get("output");
		input.get(0).put("output", data);
		input.get(0).put("status", "finish");
		input.add(input.remove(0));
		return input;
	}
}
