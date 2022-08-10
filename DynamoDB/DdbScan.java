package DynamoDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DdbScan {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.AP_NORTHEAST_2).build();
    static DynamoDB dynamoDB = new DynamoDB(client);

    public List<Map<String, Object>> retrieveItemWithFilter(Map<String, String> param, String tableName) {

        Table table = dynamoDB.getTable(tableName);

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":a", param.get("param1"));
        expressionAttributeValues.put(":b", param.get("param2"));

        try {
            ItemCollection<ScanOutcome> items = table.scan(
                    // FilterExpression
                    "user_id = :a and user_name = :b",
                    // item -> item
                    // parameter -> :parameter
                    // query item connect by and, or
                    // expression can use '>=, <=, =, != ...'

                    // ProjectionExpression
                    "user_id, user_name, user_email",
                    // what you want to scan property
                    null, // ExpressionAttributeNames - not used in this example
                    expressionAttributeValues);

            // response data parsing
            List<Map<String, Object>> returnedList = new ArrayList<Map<String, Object>>();

            if (items.getMaxResultSize() != null && items.getMaxResultSize() < 1) {
                Iterator<Item> iterator = items.iterator();
                while (iterator.hasNext()) {
                    returnedList.add(iterator.next().asMap());
                }

                return returnedList;
            } else {
                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

}