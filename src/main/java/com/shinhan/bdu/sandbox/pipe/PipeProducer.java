package com.shinhan.bdu.sandbox.pipe;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.LogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.step.GetHdfsBizAndFileInfoStep;
import com.shinhan.bdu.sandbox.util.CollectionUtil;
import com.shinhan.bdu.sandbox.util.FileUtil;
import com.shinhan.bdu.sandbox.util.JsonUtil;
import com.shinhan.bdu.sandbox.util.MetaReadUtil;
public class PipeProducer {
	
	private final Logger logger = LoggerFactory.getLogger(PipeProducer.class);
	private static PipeProducer instance;
	
	private PipeProducer() {
	}
	
	synchronized public static PipeProducer getInstance() {
        try {
            if (instance == null) {
                instance = new PipeProducer();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return instance;
    }
	
	@SuppressWarnings("static-access")
	private Map<String, Object> getProcessMap(String processMetaFileName) {
		String prcsJsonStr = MetaReadUtil.getInstance().readJsonFile(processMetaFileName);
		return JsonUtil.getMapFeomJsonString(prcsJsonStr);
	} 
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Pipeline makePipeLine(List<String> clsNames) throws ClassNotFoundException {
		
		Pipeline pipe = new Pipeline(ClassBuilder.getInstance().makeClass(clsNames.get(0)));
		for (String cls : clsNames.subList(1, clsNames.size())) {
			pipe = pipe.pipe(ClassBuilder.getInstance().makeClass(cls));
		}
		return pipe;
		
	} 
	
	public boolean runPipeLine(String processMetaFileName) {
		Map<String, Object> process = getProcessMap(processMetaFileName);
		CollectionUtil.loggingPrintMap("Read Process Meta", process, logger);
		
		
		try {
			Pipeline pipeline = makePipeLine((List<String>)process.get("pipe"));
			List<Map<String, Object>> out = (ArrayList<Map<String, Object>>) pipeline.execute(process.get("meta"));
			
			Map<String, String> config = MetaReadUtil.getInstance().readBasicConfig();
			
			String outFileName = processMetaFileName + "_" + CollectionUtil.getDay() + "_sdbx_mnt.json";
			File file = new File(config.get("processSnapShotPath"), outFileName);
			Path path = Paths.get(file.getAbsolutePath());
			Files.createDirectories(path.getParent());
			if(file.exists()) {
				logger.warn("%s is aleady exist, system Overwrite it.", outFileName);
			} 
			
			JsonUtil.getJsonFileFromListMap(file, out);
			
			return true;
		} catch (Exception ex) {
			/*
			 * TODO ClassNotFoundException외 exception 세분화.
			 */
			logger.error("{}", new ExceptionHandler().getPrintStackTrace(ex));
			return false;
		}
	}
	
	
	
}
