package com.shinhan.bdu.sandbox.pipe;

import java.lang.reflect.InvocationTargetException;

import com.shinhan.bdu.sandbox.step.abstact.Step;

public class ClassBuilder {
	
	private static ClassBuilder instance;
	
	private ClassBuilder() {
	}
	
	synchronized public static ClassBuilder getInstance() {
        try {
            if (instance == null) {
                instance = new ClassBuilder();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return instance;
    }
	 
	@SuppressWarnings("rawtypes")
	public Step makeClass(String clsname) throws ClassNotFoundException {
		String pullPath = "com.shinhan.bdu.sandbox.step." + clsname;
		try {
			return (Step)Class.forName(pullPath).getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	    
	}

}
