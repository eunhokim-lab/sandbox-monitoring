package com.shinhan.bdu.sandbox.step.prd;

/**
 *  
 * @desc Workflow Pipelining Step (실제 동작 모듈), exception handdling 담당 
 *
 */
public interface Step<I, O> {
	
    @SuppressWarnings("serial")
	public static class StepException extends RuntimeException {
        public StepException(Throwable t) {
            super(t);   
        }
    }
   
/**
 * @desc : Overriding with implementation
 * @do : actual processing logic
 *  
 */
    public O process(I input) throws StepException;
 
        
}

