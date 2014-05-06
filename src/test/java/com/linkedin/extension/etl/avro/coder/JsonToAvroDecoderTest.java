package com.linkedin.extension.etl.avro.coder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import junit.framework.Assert;
import mockit.Deencapsulation;
import mockit.Expectations;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.extension.etl.avro.transformer.JsonToAvroTransformer;
import com.linkedin.extension.hadoop.hdfs.AbstractHDFSTest;


public class JsonToAvroDecoderTest extends AbstractHDFSTest {
	static private Log LOG = LogFactory.getLog(JsonToAvroDecoderTest.class);
	private String filePath;
	private String schemaName = "price.avsc";
	private String dataName = "price.json";
	
	@Before
	public void setup() throws Exception {
		super.initJunitModeTest();
		this.filePath = ROOT_DIR + schemaName;
		super.writeHDFSFile("/" + schemaName, this.filePath);
	}
	
	@After 
	public void tearDown() {
		super.shutdown();
	}
	
	@Test 
	public void getAvroSchema() throws IOException {
		final JsonToAvroDecoder decoder = new JsonToAvroDecoder();
		
		new Expectations() {
		      {
		    	  Schema schema = Deencapsulation.invoke(decoder, "getAvroSchema", filePath);
		    	  Assert.assertEquals(schema.getField("price_id").name(), "price_id");
		      }
		   };
	}
	
	@Test
	public void getTimestamp() throws Exception {
		String path = getClass().getResource("/" + dataName ).getFile();
		JsonObject price = new JsonParser().parse(new BufferedReader(new FileReader(path))).getAsJsonObject();
		String schemaStr = readFileAsString("/" + schemaName);
		Schema.Parser parser = new Schema.Parser();

        Schema schema = parser.parse(schemaStr);	 
        
        JsonObject nestedSchema = JsonToAvroTransformer.AvroSchemaHelper.getNestedJsonSchema(schema);
        JsonToAvroTransformer transformer = new JsonToAvroTransformer(nestedSchema, schema);
        
        final Record r = transformer.transform(price.getAsJsonObject("_blob"));
		final JsonToAvroDecoder decoder2 = new JsonToAvroDecoder();
		new Expectations() {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		      {
		    	  Deencapsulation.setField(decoder2, format);
		    	  long time = Deencapsulation.invoke(decoder2, "getTimestamp", r, 
		    			  "price_meta_modifiedTs, price_meta_createdTs");
		    	  LOG.debug("time = " + new Date(time));
		    	  System.out.println("time = " + new Date(time));
//		    	  Assert.assertEquals(expected, actual);
		      }
		   };
	}
	
	@Test 
	public void decode() throws Exception {
		final JsonToAvroDecoder decoder = new JsonToAvroDecoder();
		decoder.init(new Properties(), "");
		String path = getClass().getResource("/" + dataName).getFile();
		JsonObject price = new JsonParser().parse(new BufferedReader(new FileReader(path))).getAsJsonObject();
		JsonArray arr = new JsonArray();
		arr.add(price);
		CamusWrapper<Record> wrapper = decoder.decode(arr.toString().getBytes());
		LOG.debug("timestamp: " + new Date(wrapper.getTimestamp()));
		LOG.debug("Record: " + wrapper.getRecord());
	}
}
