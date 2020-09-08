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
import com.shinhan.bdu.sandbox.step.GetHdfsSandboxInfoStep;
import com.shinhan.bdu.sandbox.step.GetTableUsageStep;
import com.shinhan.bdu.sandbox.step.Step;
public class ProcessMain {
	
	private final static Logger logger = LoggerFactory.getLogger(ProcessMain.class);
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
    	
    	/*
    	System.out.println("****** TEST PIPELINE");
        System.out.println(PipeProducer.getInstance().runPipeLine("testprocess"));
        
        System.out.println("****** TEST TABLE USAGE MODULE");
        Pipeline<List<Map>, List<Map>> pipeline1 = new Pipeline(new GetTableUsageStep());
        pipeline1.execute(new ArrayList<Map>());
        PipeProducer.getInstance().runPipeLine("usagetest");
        
        System.out.println("****** TEST HDFS META MODULE");
        Pipeline<List<Map>, List<Map>> pipeline2 = new Pipeline(new GetHdfsInfoStep());
        pipeline2.execute(new ArrayList<Map>());
        PipeProducer.getInstance().runPipeLine("hdfstest");
        
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("sandbox_process");
    	
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("test_maria");
    	
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("biz_file_process");
    	
    	
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("sandbox_oss_process");
    	
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("biz_file_oss_process");
    	
    	System.out.println("****** TEST PRCESSING MODULE");
    	PipeProducer.getInstance().runPipeLine("quota_test");
    	*/
    	
    	logger.info(" [ ****** RUN : sandbox_oss_process ]");
    	PipeProducer.getInstance().runPipeLine("sandbox_oss_process");
    	
    	logger.info(" [ ****** RUN :  biz_file_oss_process ] ");
    	PipeProducer.getInstance().runPipeLine("biz_file_oss_process");
    	
    	logger.info(" [ ****** RUN :  sandbox_quota_oss_process ] ");
    	PipeProducer.getInstance().runPipeLine("sandbox_quota_oss_process");
    	
    }
}