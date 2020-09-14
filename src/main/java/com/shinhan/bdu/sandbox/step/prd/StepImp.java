package com.shinhan.bdu.sandbox.step.prd;

import java.util.List;
import java.util.Map;

import com.shinhan.bdu.sandbox.step.prd.Step.StepException;

public abstract class StepImp<I , O> implements Step<I , O>{
	
	@Override
	public O process(I input) throws StepException { 
		before(input);
		Object data = logic(input);
		return post(input, data);
	}
	
	protected Object logic(I input) throws StepException { return null; }
	protected O post(I input, Object data) throws StepException { return null; }
	protected void before(I input) throws StepException {}
	
}
