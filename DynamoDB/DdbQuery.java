package DynamoDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DdbQuery {

    private static final Logger logger = LoggerFactory.getLogger(DdbQuery.class);

    // Region region = Region.AP_NORTHEAST_2;
    // DynamoDbClient ddb = DynamoDbClient.builder().region(region).build();

    public void insertItem(DynamoDbClient ddb, Map<String, AttributeValue> param, String tableName) {
        PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(param).build();

        ddb.putItem(request);
        logger.info("inserted item");
    }

    public void updateItem(DynamoDbClient ddb, Map<String, String> param, String tableName, String key,
            String keyValue) {
        updateTableItem(ddb, param, tableName, key, keyValue);
    }

    public void updateTableItem(DynamoDbClient ddb, Map<String, String> param, String tableName, String key,
            String keyValue) {

        HashMap<String, AttributeValue> itemKey = new HashMap<String, AttributeValue>();
        itemKey.put(key, AttributeValue.builder().s(keyValue).build());

        HashMap<String, AttributeValueUpdate> updatedValues = new HashMap<String, AttributeValueUpdate>();

        for (Entry<String, String> elem : param.entrySet()) {

            updatedValues.put(elem.getKey(),
                    AttributeValueUpdate.builder().value(AttributeValue.builder().s(elem.getValue().toString()).build())
                            .action(AttributeAction.PUT).build());
        }

        UpdateItemRequest request = UpdateItemRequest.builder().tableName(tableName).key(itemKey)
                .attributeUpdates(updatedValues).build();

        try {
            ddb.updateItem(request);
            logger.info("updated item");
        } catch (ResourceNotFoundException e) {
            e.printStackTrace();
        } catch (DynamoDbException e) {
            e.printStackTrace();
        }
    }

    public Map<String, AttributeValue> retrieveItem(DynamoDbClient ddb, Map<String, String> param, String tableName) {
        String key = "id";
        String keyVal = param.get("id").toString();

        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
        keyToGet.put(key, AttributeValue.builder().s(keyVal).build());
        GetItemRequest request = GetItemRequest.builder().key(keyToGet).tableName(tableName).build();
        Map<String, AttributeValue> returnedItem = null;

        returnedItem = ddb.getItem(request).item();

        if (returnedItem != null) {
            Set<String> keys = returnedItem.keySet();
            logger.info("Amazon DynamoDB table attributes: \n");

            for (String key1 : keys) {
                System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
            }
        } else {
            System.out.format("No item found with the key %s!\n", key);
        }

        return returnedItem;
    }

}
