package example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class CreateMessage implements RequestHandler<S3Event, String> {

    private static Logger LOGGER = LoggerFactory.getLogger(CreateMessage.class);

    private DynamoDB dynamoDb;
    private AmazonSQS sqs;
    private String QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/826395435023/SQSForXML";
    private String DYNAMODB_TABLE_NAME = "Files";
    private Regions REGION = Regions.EU_WEST_1;
    private String STATUS = "UPLOADED";

    public String handleRequest(S3Event s3Event, Context context) {
        try {
            String message = getS3BucketBody(s3Event);
            createDBRecord(s3Event);
            createSQSMessage(message, s3Event);

            return "Success!";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private void createDBRecord(S3Event s3Event) throws AWSException {
        try {
            this.initDynamoDbClient();
            persistData(s3Event);
        } catch (Exception e) {
            throw new AWSException("Error while trying to save record to DB.");
        }
        LOGGER.info("Record successfully added to DB table");
    }

    private void persistData(S3Event s3event) throws AWSException {
        LocalDate today = LocalDate.now();

        try {
            this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
                    .putItem(new Item()
                            .withPrimaryKey("fileName", getS3BucketFileName(s3event))
                            .withString("date", today.toString())
                            .withString("file_status", STATUS));
        }catch (Exception e) {
            throw new AWSException("Error while writing data to DynamoDB: " + e);
        }

    }

    private String getS3BucketBody(S3Event s3Event) throws AWSException {
        String body;
        try {
            S3EventNotification.S3EventNotificationRecord record = getS3Record(s3Event);

            String bkt = record.getS3().getBucket().getName();
            LOGGER.info("Bucket name [{}]", bkt);

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

            LOGGER.info("Getting body...");
            body = s3Client.getObjectAsString(bkt, getS3BucketFileName(s3Event));
        } catch (Exception e) {
           throw new AWSException("Error while getting S3Bucket uploaded file body: " + e);
        }
        LOGGER.info("Message received from S3 Bucket [{}]", body);
        return body;
    }

    private void createSQSMessage(String message, S3Event s3Event) throws AWSException {
        initSQSClient();

        LOGGER.info("Sending msg to SQS [{}]", message);
        SendMessageResult smr;

        try {
            String fileName = getS3BucketFileName(s3Event);

            Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
            messageAttributes.put(fileName, new MessageAttributeValue().withDataType("String").withStringValue("File"));

            smr = sqs.sendMessage(new SendMessageRequest()
                    .withMessageBody(message)
                    .withQueueUrl(QUEUE_URL)
                    .withMessageAttributes(messageAttributes));
        }catch (Exception e) {
            throw new AWSException("Error while creating SQS message: " + e);
        }

       LOGGER.info("Creation of SQS message succeeded with messageId [{}]", smr.getMessageId());
    }

    private String getS3BucketFileName(S3Event s3event) throws AWSException {
        String key;
        try {
            S3EventNotification.S3EventNotificationRecord record = getS3Record(s3event);

            LOGGER.info("Getting key of S3 Bucket file...");
            key = record.getS3().getObject().getKey().replace('+', ' ');
            key = URLDecoder.decode(key, "UTF-8");
        } catch (Exception e) {
            throw new AWSException("Error while getting S3Bucket name: " + e);
        }
        LOGGER.info("Key successfully received [{}]", key);
        return key;
    }

    private void initDynamoDbClient() throws AWSException {
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
            this.dynamoDb = new DynamoDB(client);
        } catch (Exception e) {
            throw new AWSException("Error while initializing DynamoDBClient: " + e);
        }
    }

    private void initSQSClient() throws AWSException {
        try {
            this.sqs = AmazonSQSClientBuilder.standard()
                    .withRegion(REGION)
                    .build();
        }catch (Exception e) {
            throw new AWSException("Error while initializing SQSClient: " + e);
        }
    }

    private S3EventNotification.S3EventNotificationRecord getS3Record(S3Event s3Event) throws AWSException {
        if (!s3Event.getRecords().isEmpty())
            return s3Event.getRecords().get(0);
        throw new AWSException("Error while getting S3 Event Notification record");
    }


}
