package com.linkedin.extension.hadoop.hdfs;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSFileUtilTest extends AbstractHDFSTest {
	static private Log LOG = LogFactory.getLog(AbstractHDFSTest.class);
	private String filePath;
	private String fileName= "Price.avsc";

	@Before
	public void setup() throws Exception {
		super.initJunitModeTest();
		this.filePath = ROOT_DIR + fileName;
		super.writeHDFSFile("/"  + fileName, this.filePath);
	}

	@After
	public void tearDown() {
		super.shutdown();
	}

	@Test
	public void readFileAsString() throws IOException {
		String schemaStr = HDFSFileUtil.readFileAsString(filePath);
		LOG.info("HDFS avro schema:" + schemaStr);
		String fileSchemaStr = readFileAsString("/Price.avsc");
		LOG.info("File avro schema:" + fileSchemaStr.length());
		Assert.assertEquals(fileSchemaStr, schemaStr);
	}
}
