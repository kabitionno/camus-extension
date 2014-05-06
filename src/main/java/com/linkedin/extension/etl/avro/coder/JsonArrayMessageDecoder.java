package com.linkedin.extension.etl.avro.coder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;


/**
 * MessageDecoder class that will convert the payload into a JSON object,
 * look for a field named 'timestamp', and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp, then System.currentTimeMillis() will be used.
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class JsonArrayMessageDecoder extends MessageDecoder<byte[], String[]> {
	private static Logger log = Logger.getLogger(JsonArrayMessageDecoder.class);
        private static JsonParser  parser = new JsonParser();
        private Properties props;
        private String topicName;

	@Override
	public void init(Properties props, String topicName) {
		this.props     = props;
		this.topicName = topicName;
	}

	@Override
	public CamusWrapper<String[]> decode(byte[] payload) {
		String payloadString =  new String(payload);
                List<String> list = new ArrayList<String>();
                String jsonStr;
                String[] jsonStrArray;
		// Parse the payload into a JsonObject.
		try {
		      JsonArray  jsonArray =  parser.parse(payloadString).getAsJsonArray();
		      if(jsonArray != null && jsonArray.size() > 0) {
			  for(int i =0; i < jsonArray.size(); i++) {
                              jsonStr = jsonArray.get(i).getAsJsonObject().toString();
                              if(jsonStr != null && !jsonStr.isEmpty()) {
                                 jsonStr = jsonStr.replaceAll("\\\\r|\\\\n|\\\\t", "");
                                 list.add(jsonStr);
	                      } else {
                                 log.error("Skipped Json:" + jsonStr);
                              }	
                          }
                     }
                } catch (RuntimeException e) {
			log.error("Caught exception while parsing JSON array:" + payloadString );
                        return null;
		}
                

                if(!list.isEmpty()) {
                    jsonStrArray = list.toArray(new String[list.size()]); 
		    return new CamusWrapper<String[]>(jsonStrArray);
                } else 
                    return null;
	}
}
