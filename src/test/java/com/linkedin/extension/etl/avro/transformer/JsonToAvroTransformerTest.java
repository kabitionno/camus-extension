package com.linkedin.extension.etl.avro.transformer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.linkedin.extension.etl.avro.TestFileUtil;


public class JsonToAvroTransformerTest {
	static private Log LOG = LogFactory.getLog(JsonToAvroTransformerTest.class);
	private Schema schema;
	private JsonObject nestedSchema;
	private JsonParser parser;
	
	@Before
    public void setup() throws IOException {
		this.parser = new JsonParser();
		this.schema = TestFileUtil.readAvroSchema("/price.avsc");
		this.nestedSchema = JsonToAvroTransformer.AvroSchemaHelper.getNestedJsonSchema(schema);
	}
	
	@Test
	public void transform() throws Exception {
		String path = getClass().getResource("/price.json").getFile();
		JsonObject price = this.parser.parse(new BufferedReader(new FileReader(path))).getAsJsonObject();
		price = price.get("_blob").getAsJsonObject();
		LOG.debug("price: " + price.toString());
		LOG.debug("schema: " + this.schema.toString());
		LOG.debug("nestedSchema: " + this.nestedSchema.toString());
		
		JsonToAvroTransformer transformer = new JsonToAvroTransformer(this.nestedSchema, this.schema);
		Record r = transformer.transform(price);
		
		LOG.debug("Record: " + r.toString());
		TestFileUtil.writeAvroRecordToByteArray(r, this.schema);
	}
	
	@Test
	public void transformSingleItemJson() throws IOException {
		String path = getClass().getResource("/singleitem.json").getFile();
		JsonObject singleItem = this.parser.parse(new BufferedReader(new FileReader(path))).getAsJsonObject();
		
		Schema schema = TestFileUtil.readAvroSchema("/singleitem.avsc");
		JsonObject jsonSchema = JsonToAvroTransformer.AvroSchemaHelper.getNestedJsonSchema(schema);
		
		JsonToAvroTransformer transformer = new JsonToAvroTransformer(jsonSchema, schema);
		Record r = transformer.transform(singleItem);
		
		LOG.debug("Record: " + r.toString());
	}
	
	@Test
	public void performance() throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		String path = getClass().getResource("/price.json").getFile();
		JsonObject price = this.parser.parse(new BufferedReader(new FileReader(path))).getAsJsonObject();
		price = price.getAsJsonObject("_blob");
		
		Record r = new GenericData.Record(this.schema);
		JsonToAvroTransformer transformer = new JsonToAvroTransformer(this.nestedSchema, this.schema);
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			transformer.transform(price);
		}
		long duration = System.currentTimeMillis() - start;
		LOG.info("Processing time for transformer: " + duration + " mills");		
		
		String jsonStr = price.toString();
		start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			this.parser.parse(jsonStr);
		}
		long duration3 = System.currentTimeMillis() - start;
		LOG.info("Processing time for gson parser: " + duration3 + " mills");
		LOG.info(duration3/duration + " times faster than gson parser");		
	}

}
