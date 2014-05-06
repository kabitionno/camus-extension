package com.linkedin.extension.etl.avro.transformer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.linkedin.extension.etl.Transformer;

/**
 * Transform a JSON object to an AVRO Record given a flatten AVRO schema.
 * All AVRO types in given schema are defined as Type.UNION, with Type.NULL 
 * as a mandatory item for backward compatibility and serialization.
 * 
 */
public class JsonToAvroTransformer implements Transformer<JsonObject, Record> {
	/**
	 * Nested json schema for avro Record object building, it is converted from
	 * flattened avro schema
	 */
	private JsonObject jsonSchema;

	private Schema avroSchema;

	public JsonToAvroTransformer(JsonObject jSchema, Schema avroSchema) {
		this.jsonSchema = jSchema;
		this.avroSchema = avroSchema;
	}

	@Override
	public Record transform(JsonObject jsonObj) {
		Record r = new GenericData.Record(avroSchema);
		transform(jsonSchema, jsonObj, r);
		
		return r;
	}

	/**
	 * Transform json to avro in flattened format. Flattening happens at the first level of the 
	 * avro schema. For array elements, one-to-one exact mapping is performed.  
	 * 
	 * @param schema
	 *            nested avro schema
	 * @param jsonObj
	 *            json object
	 * @returns r
	 *            avro record
	 */
	private void transform(JsonObject schema, JsonObject jsonObj, Record r) {
		Set<Map.Entry<String, JsonElement>> entrySet = schema.entrySet();
		for (Map.Entry<String, JsonElement> entry : entrySet) {
			String memberName = entry.getKey();
			JsonElement element = entry.getValue();
			
			if (element.isJsonObject()) {
				JsonObject subSchema = element.getAsJsonObject();
				JsonElement el = jsonObj.get(memberName);
				if (el == null) {
					continue;
				}

				JsonObject subJsonObj = el.getAsJsonObject();
				transform(subSchema, subJsonObj, r);
			} else if (element.isJsonPrimitive()) {
				JsonElement val = jsonObj.get(memberName);
				if (val != null) {
					String key = element.getAsString();
					Field field = avroSchema.getField(key);
					Schema typeSchema = getTypeSchema(field.schema()); 
					if (val.isJsonArray()) {	//shortcut to get array
						r.put(field.name(), getArray(typeSchema, val));
					} else {
						r.put(field.name(), getValue(typeSchema, val));
					}
				}
			}
		}
	}
	
	private List<Record> getArray(Schema schema, JsonElement jsonArr) {
		List<Record> avroArr = new LinkedList<Record>();
		
		Schema itemType = schema.getElementType();
		JsonArray values = jsonArr.getAsJsonArray();
		for (int i = 0; i < values.size(); i++) {
			Record child = (Record) getValue(itemType, values.get(i));
			avroArr.add(child);
		}
		
		return avroArr;
	}
	
	private Record getRecord(Schema schema, JsonElement val) {
		Record r = new GenericData.Record(schema);
		List<Field> fields = schema.getFields();
		JsonObject json = val.getAsJsonObject();
		for (Field fld : fields) {
			Schema typeSchema = getTypeSchema(fld.schema());
			JsonElement elt = json.get(fld.name());
			r.put(fld.name(), getValue(typeSchema, elt));
		}
		
		return r;
	}
	
	private Object getValue(Schema typeSchema, JsonElement val) {
		if (val == null)
			return val;
		
		switch (typeSchema.getType()) {
		case STRING:
			return val.getAsString();
		case BOOLEAN:
			return val.getAsBoolean();
		case DOUBLE:
			return val.getAsDouble();
		case LONG:
			return val.getAsLong();
		case INT:
			return val.getAsInt();
		case FLOAT:
			return val.getAsFloat();
		case ARRAY:
			return getArray(typeSchema, val);
		case RECORD: 
			return getRecord(typeSchema, val);
		default:
			throw new RuntimeException(typeSchema.getType().toString()
					+ " not supported yet");
		}
	}
	
	private Schema getTypeSchema(Schema typeUnion) {
		if (!typeUnion.getType().equals(Type.UNION)) {
			throw new RuntimeException("The type schema is not UNION type: "
					+ typeUnion.toString());
		}

		Schema typeSchema = null;
		for (Schema sch : typeUnion.getTypes()) {
			if (!sch.getType().equals(Type.NULL)) {
				typeSchema = sch;
				break;
			}
		}
		
		return typeSchema;
	}
	

	/**
	 * Helper class to transform avro schema to nested jason schema
	 * 
	 */
	public static class AvroSchemaHelper {
		static public JsonObject getNestedJsonSchema(Schema schema) {
			JsonObject flat = new JsonParser().parse(schema.toString())
					.getAsJsonObject();
			JsonObject nested = new JsonObject();

			JsonArray fields = flat.getAsJsonArray("fields");
			for (JsonElement e : fields) {
				String flatPath = e.getAsJsonObject().get("name").getAsString();
				addElements(nested, flatPath);
			}

			return nested;
		}

		static private void addElements(JsonObject nested, String flatpath) {
			String[] names = flatpath.split("\\_");
			JsonObject jsonObj = nested;
			for (int i = 0; i < names.length; i++) {
				String name = names[i];

				if (!jsonObj.has(name)) {
					jsonObj.add(name, new JsonObject());
				}

				if (i == names.length - 1) {
					jsonObj.addProperty(name, flatpath);
				} else {
					jsonObj = jsonObj.getAsJsonObject().get(name)
							.getAsJsonObject();
				}
			}
		}
	}
}