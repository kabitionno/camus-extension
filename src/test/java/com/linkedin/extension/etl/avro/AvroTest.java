package com.linkedin.extension.etl.avro;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;


public class AvroTest {
//	@Test
	public void checkBinaryJson(String json) throws Exception {
//		JsonObject price = TestFileUtil.readJsonObject("/price.json");
//		
//		JsonNode node = AvroSchema.parseJsonString(price.toString());
//	    ByteArrayOutputStream out = new ByteArrayOutputStream();
//	    DatumWriter<JsonNode> writer = new Json.Writer();
//	    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//	    encoder = EncoderFactory.get().validatingEncoder(Json.SCHEMA, encoder);
//	    writer.write(node, encoder);
//	    encoder.flush();
//	    byte[] bytes = out.toByteArray();
//	    
//	    DatumReader<JsonNode> reader = new Json.Reader();
//	    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
//	    decoder = DecoderFactory.get().validatingDecoder(Json.SCHEMA, decoder);
//	    JsonNode decoded = reader.read(null, decoder);
//	 
//	    Assert.assertEquals("Decoded json does not match.", node.toString(), decoded.toString());
	  }
	
//	@Test
	public void checkJson(String json) throws Exception {
//		JsonObject price = TestFileUtil.readJsonObject("/price.json");
//		
//		JsonNode node = AvroSchema.parseJsonString(price.toString());
//	    ByteArrayOutputStream out = new ByteArrayOutputStream();
//	    DatumWriter<JsonNode> writer = new Json.Writer();
//	    Encoder encoder = EncoderFactory.get().jsonEncoder(Json.SCHEMA, out);
//	    encoder = EncoderFactory.get().validatingEncoder(Json.SCHEMA, encoder);
//	    writer.write(node, encoder);
//	    encoder.flush();
//	    byte[] bytes = out.toByteArray();
	  }
	
//	@Test
	public void jsonToAvro() throws Exception {	
		Schema inputSchema = TestFileUtil.readAvroSchema("/input_orig.avsc");
		String path = getClass().getResource("/input.json").getFile();
		File f = new File(path);
		List<Object> objs = fromJson(inputSchema, f);
		for (Object o : objs) {
			System.out.println("Object:" + o.getClass());
		}
	}

	@Test(expected=NullPointerException.class)
	public void avroArray() throws Exception {
		Schema inputSchema = TestFileUtil.readAvroSchema("/input.avsc");
		
		Record r = new GenericData.Record(inputSchema);
		Field fld = inputSchema.getField("DocId");
		r.put(fld.name(), 10L);
		
		fld = inputSchema.getField("Name");	
		Schema nameSchema = null;
		List<Record> nameList = new ArrayList<Record>();
		for (Schema type : fld.schema().getTypes()) {
			if (type.getType().equals(Type.ARRAY)) {
				nameSchema = type;
			}
		}
	
		Record nameRecord = new GenericData.Record(nameSchema.getElementType());
		nameList.add(nameRecord);
		Record nameRecord2 = new GenericData.Record(nameSchema.getElementType());
		nameList.add(nameRecord2);
		r.put(fld.name(), nameList);
		
		List<Field> fields = nameSchema.getElementType().getFields();
		fld = nameSchema.getElementType().getField("Language");
		System.out.println("field name:" + fld.name() + ", field type:" + fld.schema().getType());
		if (fld.schema().getType().equals(Type.ARRAY)) {
			List<Record> langList = new ArrayList<Record>();
			nameRecord.put(fld.name(), langList);
			Record langRecord = new GenericData.Record(fld.schema().getElementType());
			langList.add(langRecord);
			
			Field codeFld = fld.schema().getElementType().getField("Code");
			langRecord.put(codeFld.name(), "lang code");
			
			Field countryFld = fld.schema().getElementType().getField("Country");
			System.out.println("Country field type:" + countryFld.schema().getType());
			langRecord.put(countryFld.name(), "USA");
			
			Record langRecord2 = new GenericData.Record(fld.schema().getElementType());
			langList.add(langRecord2);
			
			
			langRecord2.put(codeFld.name(), "lang code");
		}
		
		System.out.println("Record: " + r);
		TestFileUtil.writeAvroRecordToByteArray(r, inputSchema);
	}
	
	private List<Object> fromJson(Schema schema, File file) throws Exception {
	    InputStream in = new FileInputStream(file);
	    List<Object> data = new ArrayList<Object>();
	    try {
	      DatumReader reader = new GenericDatumReader(schema);
	      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, in);
	      while (true)
	        data.add(reader.read(null, decoder));
	    } catch (EOFException e) {
	    } finally {
	      in.close();
	    }
	    return data;
	  }
	
	
}
