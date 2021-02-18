package com.productiveanalytics.avro;

import org.apache.avro.Schema;
import org.apache.avro.util.RandomData;

import java.util.Iterator;
import java.util.Objects;

public final class GenericAVROMessageProducer {

	public static void main(String[] args) {
		String TEST_SCHEMA = 
				"{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.productiveanalytics.avro\",\n" +
                "     \"name\": \"TestUser\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"userId\", \"type\": \"long\" },\n" +
                "       { \"name\": \"name\", \"type\": \"string\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\" },\n" +
                "       { \"name\": \"sex\", \"type\": \"string\" },\n" +
                "       { \"name\": \"active\", \"type\": \"boolean\" }\n" +
                "     ]\n" +
                "}";
		String msg = generateMessage(TEST_SCHEMA);
		System.out.println(msg);
	}
	
	private static String generateMessage(String avroSchemaContent) {
		Objects.requireNonNull(avroSchemaContent, "Need valid AVRO schema content");
        Schema schema = new Schema.Parser().parse(avroSchemaContent);

        Iterator<Object> it = new RandomData(schema, 1, 1L).iterator();
        String jsonMsg = it.next().toString();
        return jsonMsg;
	}

}
