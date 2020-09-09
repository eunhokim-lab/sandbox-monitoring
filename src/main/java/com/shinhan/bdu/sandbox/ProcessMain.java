package com.shinhan.bdu.sandbox;

import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.http.HttpClient;
import com.shinhan.bdu.sandbox.pipe.ClassBuilder;
import com.shinhan.bdu.sandbox.pipe.PipeProducer;
import com.shinhan.bdu.sandbox.pipe.Pipeline;
import com.shinhan.bdu.sandbox.step.GetHdfsSandboxDbInfoStep;
import com.shinhan.bdu.sandbox.step.GetTableUsageStep;
import com.shinhan.bdu.sandbox.step.Step;
public class ProcessMain {
	
	private final static Logger logger = LoggerFactory.getLogger(ProcessMain.class);
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
    	
    	/*
    	logger.info(" [ ****** RUN : sandbox_oss_process ]");
    	PipeProducer.getInstance().runPipeLine("sandbox_oss_process");
    	
    	logger.info(" [ ****** RUN :  biz_file_oss_process ] ");
    	PipeProducer.getInstance().runPipeLine("biz_file_oss_process");
    	
    	logger.info(" [ ****** RUN :  sandbox_quota_oss_process ] ");
    	PipeProducer.getInstance().runPipeLine("sandbox_quota_oss_process");
    	
    	logger.info(" [ ****** RUN :  sandbox_file_oss_process ] ");
    	PipeProducer.getInstance().runPipeLine("sandbox_file_oss_process");
    	*/
    	
    	
    	logger.info(" [ ****** RUN : sandbox_process ]");
    	PipeProducer.getInstance().runPipeLine("sandbox_process");
    	
    	logger.info(" [ ****** RUN :  biz_file_process ] ");
    	PipeProducer.getInstance().runPipeLine("biz_file_process");
    	
    	logger.info(" [ ****** RUN :  sandbox_quota_process ] ");
    	PipeProducer.getInstance().runPipeLine("sandbox_quota_process");
    	
    	logger.info(" [ ****** RUN :  sandbox_file_process ] ");
    	PipeProducer.getInstance().runPipeLine("sandbox_file_process");
    	
    }
    
}