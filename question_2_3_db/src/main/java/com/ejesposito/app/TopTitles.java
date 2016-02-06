package com.ejesposito.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.ArrayList;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

public class TopTitles {

    static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());

    public static void main(String[] args) throws Exception {
        try {
	    DynamoDBMapper mapper = new DynamoDBMapper(client);

	    BufferedReader br = null;
            String cvsSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

	    ArrayList<AirportCarries> list = new ArrayList<AirportCarries> ();

            br = new BufferedReader(new FileReader("part-r-00000"));
            int i = 0;
	    String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(cvsSplitBy);
                AirportCarries ac1 = new AirportCarries();
                ac1.id = i;
                ac1.origin = data[0];
                ac1.destiny = (data[1].split("\""))[1];
		ac1.carrier = (data[1].split("\""))[3];
		ac1.value = data[2];
		list.add(ac1);
		i++;
            }
	    br.close();

	    mapper.batchSave(list);

	    System.out.println("Example complete!");
        } catch (Throwable t) {
            System.err.println("Error running the DynamoDBMapperBatchWriteExample: " + t);
            t.printStackTrace();
        }
    }

    @DynamoDBTable(tableName="OriDes")
    public static class AirportCarries {
            private int id;
            private String origin;
	    private String destiny;
            private String carrier;
	    private String value;

            //Partition key
            @DynamoDBHashKey(attributeName="Id")
            public int getId() { return id; }
            public void setId(int id) { this.id = id; }

            @DynamoDBAttribute(attributeName="Origin")
            public String getOrigin() { return origin; }
            public void setOrigin(String origin) { this.origin = origin; }

            @DynamoDBAttribute(attributeName="Destiny")
            public String getDestiny() { return destiny; }
            public void setDestiny(String destiny) { this.destiny = destiny; }

            @DynamoDBAttribute(attributeName="Carrier")
            public String getCarrier() { return carrier; }
            public void setCarrier(String carrier) { this.carrier = carrier; }

            @DynamoDBAttribute(attributeName="Value")
            public String getValue() { return value; }
            public void setValue(String value) { this.value = value; }

     }

}
