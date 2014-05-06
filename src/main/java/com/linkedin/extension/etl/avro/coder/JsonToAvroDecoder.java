package com.linkedin.extension.etl.avro.coder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.extension.etl.avro.transformer.JsonToAvroTransformer;
import com.linkedin.extension.etl.Transformer;
import com.linkedin.extension.hadoop.hdfs.HDFSFileUtil;

/**
 * Transform JSON array with single JSON object as an byte array to AVRO Record. 
 */
public class JsonToAvroDecoder extends MessageDecoder<byte[], Record> {
	static private Log LOG = LogFactory.getLog(JsonToAvroDecoder.class);
	public final static String AVRO_SCHEMA_FILE_PATH = "avro.schema.file.path";
	public final static String JSON_TIMESTAMP_DATE_FORMAT = "json.timestamp.date.format";
	public final static String JSON_TIMESTAMP_KEYS = "json.timestamp.keys";
	public final static String JSON_ROOT_NODE_PATH = "json.root.node.path";
	
	protected SimpleDateFormat format;
	protected String timestampKeys;
	protected JsonParser parser;
	protected SchemaRegistry<Schema> registry;
	protected Schema avroSchema;
	protected Transformer<JsonObject, Record> transformer;

	@Override
	public void init(Properties props, String topicName) {
		super.init(props, topicName);

		String schemaName = props.getProperty(AVRO_SCHEMA_FILE_PATH, "/tmp/Price.avsc").trim();
		LOG.info("Avro Schema:" + schemaName);
		try {
			this.avroSchema = getAvroSchema(schemaName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MessageDecoderException(e);
		}
			
		parser = new JsonParser();
		String formatStr = props.getProperty(JSON_TIMESTAMP_DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format = new SimpleDateFormat(formatStr);
		
		timestampKeys = props.getProperty(JSON_TIMESTAMP_KEYS, "price_meta_modifiedTs, price_meta_createdTs");
		
		JsonObject nestedJsonSchema = JsonToAvroTransformer.AvroSchemaHelper.getNestedJsonSchema(avroSchema);
		transformer = new JsonToAvroTransformer(nestedJsonSchema, this.avroSchema);
	}

	@Override
	public CamusWrapper<Record> decode(byte[] message) {
		JsonObject jsonObj = getJsonObject(message);
		if (jsonObj == null)
			return null;
		
		Record r = transformer.transform(jsonObj);
		long timestamp = getTimestamp(r, timestampKeys);
		
		return new CamusWrapper<Record>(r, timestamp);
	}
	
	/**
	 * Converts timestamp string to long. For invalid or nonexist, return system time.
	 * 
	 * @param r json record
	 * @param keys timestamp key string
	 * @return timestamp of the json record
	 */
	private long getTimestamp(Record r, String keyStr) {
		String[] keys = keyStr.split(",");
		
		Object o = null;
		for (String key: keys) {
			o = r.get(key.trim());
			if (o != null && ((String) o).length() > 0)
				break;
		}
		
		if (o == null) {
			return System.currentTimeMillis();
		}

		try {
			return format.parse((String)o).getTime();
		} catch (ParseException e) {
			return System.currentTimeMillis();
		}
	}

	/**
	 * Returns a json object from a json array, which has only one object.
	 * 
	 * @param payload json array byte array
	 * @return a json object from a json array
	 */
	private JsonObject getJsonObject(byte[] payload) {
		if (payload == null) {
			LOG.info("payload is null");
			return null;
		}
			
		JsonObject jsonObj = null;
		String payloadString = new String(payload);
		JsonArray jsonArray = parser.parse(payloadString).getAsJsonArray();
		
		if (jsonArray != null && jsonArray.size() > 0
				&& jsonArray.isJsonArray()) {
			if (jsonArray.get(0).isJsonObject()) {
				String root = props.getProperty(JSON_ROOT_NODE_PATH, "_blob").trim();
				jsonObj = jsonArray.get(0).getAsJsonObject().getAsJsonObject(root);
			}
		}
		else
			LOG.warn("Invalid json array: " + new String(payload));

		return jsonObj;
	}
	
	
	/**
	 * Return the avro schema  by the reading the file from hadoop hdfs
	 * @param filepath hdfs file path
	 * @return the avro schema
	 * @throws IOException
	 */
	private Schema getAvroSchema(String filepath) throws IOException {
		String schemaStr = HDFSFileUtil.readFileAsString(filepath);
        Schema.Parser parser = new Schema.Parser();
		
        return parser.parse(schemaStr);	
	}
}
