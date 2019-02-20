package aws.dynamo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoService {

    private Logger LOGGER = LoggerFactory.getLogger(DynamoService.class);

    private String STATUS = "UPLOADED";
    private String TABLE_NAME = System.getenv("DYNAMODB_TABLE_NAME");
    private Regions region = Regions.EU_WEST_1;

    private DynamoDB dynamoDB = initDynamoDbClient(region);

    public void createDynamoRecord(String fileName, String date) throws AWSException {
        if (fileName == null) {
            LOGGER.error("Can't create DB record: object with key [{}] did't found in S3 bucket", fileName);
            throw new AWSException("Can't create DB record: object with key [" + fileName + "] did't found in S3 bucket");
        }

        if (date == null) {
            LOGGER.error("Can't create DB record: date is null");
            throw new AWSException("Can't create DB record: date is null");
        }

        try {
            dynamoDB.getTable(TABLE_NAME)
                    .putItem(new Item()
                            .withPrimaryKey("fileName", fileName, "date", date)
                            .withString("file_status", STATUS));
            LOGGER.info("PutItem succeeded: fileName [{}], date [{}]", fileName, date);
        } catch (Exception e) {
            LOGGER.error("Unable to add item: " + fileName + " " + date + " in " + TABLE_NAME);
            LOGGER.error(e.getMessage());
            throw new AWSException("Unable to add item: " + fileName + " " + date + " in " + TABLE_NAME);
        }
    }

    private DynamoDB initDynamoDbClient(Regions region) {
        DynamoDB dynamoDB = null;
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
            dynamoDB = new DynamoDB(client);
        } catch (Exception e) {
            LOGGER.error("Error while initializing DynamoDBClient: " + e.getMessage());
        }
        return dynamoDB;
    }
}
