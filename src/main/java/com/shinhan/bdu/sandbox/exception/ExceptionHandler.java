package com.shinhan.bdu.sandbox.exception;

import org.slf4j.Logger;

public class ExceptionHandler {
	
	public void webHdfsNullExHandling(Logger logger, String url) {
		String msg = String.format("%s :: call is return null data.", url);
		logger.error(msg);
	}
	
	public void webHdfsDataNotEnoughExHandling(Logger logger, String info) {
		String msg = String.format("%s is not contain data return data will be empty. check data Info.", info);
		logger.warn(msg);
	}
	
	public void emptyRatioExHandling(Logger logger, String info) {
		String msg = String.format("%s ratio is too high to process. You needs to check about it.", info);
		logger.warn(msg);
	}
}
