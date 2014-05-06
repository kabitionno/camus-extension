package com.linkedin.extension.etl.avro.transformer;

import java.util.Map;
import java.util.Set;
import com.linkedin.extension.etl.Transformer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class JsonToAvroSchemaTransformer implements Transformer<JsonObject, JsonObject>{
	private String jsonName;
	
	public JsonToAvroSchemaTransformer(String jsonName) {
		this.jsonName = jsonName;
	}

	@Override
	public JsonObject transform(JsonObject s) {
		JsonObject r = new JsonObject();
		r.addProperty("name", jsonName);
		transform(s, r);
		return r;
	}
	
	private void transform(JsonObject jsonSchema, JsonObject avroSchema) {
		Set<Map.Entry<String, JsonElement>> entrySet = jsonSchema.entrySet();
		for (Map.Entry<String, JsonElement> entry : entrySet) {
			String memberName = entry.getKey();
			JsonElement element = entry.getValue();
			System.out.println(memberName + ":" + element.toString());
		}
	}
}
