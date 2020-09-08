package com.shinhan.bdu.sandbox.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {
	
	private static FileUtil instance = null;
	private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);
	
	private FileUtil() {
	}

	synchronized public static FileUtil getInstance() {
		if (instance == null) {
			instance = new FileUtil();
		}
		return instance;
	}
	
	
	public List<String> fileRead(String filePath) {
		List<String> lines = null;
		try {
			lines = Files.readAllLines(Paths.get(filePath),StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (lines == null)
			logger.error("File not found exception : {}", filePath);
		if (lines.size() < 1)
			logger.error("File not contain contents exception : {}", filePath);

		return lines;
	}
	
	public void fileWrtie(String filePath, Object contents) {
		Path input = Paths.get(filePath);
		try (FileChannel channel = FileChannel.open(input, StandardOpenOption.WRITE
														 , StandardOpenOption.CREATE)) {
					
			// ByteBuffer 생성 (Direct)
			ByteBuffer buf = ByteBuffer.allocateDirect(100);
			Charset charset = Charset.defaultCharset();
			
			// 파일 쓰기
			buf = charset.encode(contents.toString());
			channel.write(buf);
			
		} catch (Exception e) {

			logger.error("File write err : {}", filePath);
			e.printStackTrace();
		}
	}
	
	
	
}
