package com.linkedin.extension.etl.avro;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TestFileUtil {
	static final private JsonParser jsonParser = new JsonParser();
	
	static public JsonObject readJsonObject(String filePath) throws Exception {
		String path = JsonParser.class.getResource(filePath).getFile();
		return jsonParser.parse(new BufferedReader(new FileReader(path)))
				.getAsJsonObject();
	
	}
	
	static public Schema readAvroSchema(String fileName) throws IOException {
		String path = JsonParser.class.getResource(fileName).getFile();
        Schema.Parser parser = new Schema.Parser();
		
        return parser.parse(new File(path));	
	}
	
	static public void writeAvroRecordToByteArray(Record r, Schema schema) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(r, encoder);
        encoder.flush();
        out.close();
	}
}
