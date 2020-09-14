package com.shinhan.bdu.sandbox.step.prd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.step.prd.Step.StepException;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;

public abstract class InsertDbAccessStepImpl extends DbAccessStepImpl{
	
	protected abstract List<ArrayList<String>> getInsertDataFromPreSteps(List<Map> input) ;
	
	@Override
	protected String getQuery(List<Map> input) {
		String qry = (String) input.get(0).get("query");
		return MetaReadUtil.getInstance().readSql(qry);
	}
	
}



