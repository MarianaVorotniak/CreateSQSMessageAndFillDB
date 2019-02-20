import aws.dynamo.DynamoService;
import aws.s3.util.S3Util;
import aws.sqs.util.SqsUtil;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import aws.sqs.SQSService;

public class Main implements RequestHandler<S3Event, String> {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private DynamoService dynamoService = new DynamoService();
    private SQSService sqsService = new SQSService();

    public String handleRequest(S3Event s3Event, Context context) {
        LOGGER.info("Event received: " + s3Event.toJson());
        try {
            S3EventNotification.S3EventNotificationRecord s3Record = S3Util.getS3Record(s3Event);

            String fileName = S3Util.getFileName(s3Record);
            String bucketName = S3Util.getS3BucketName(s3Record);
            String date = S3Util.getDate();

            dynamoService.createDynamoRecord(fileName, date);
            sqsService.sendMessage(SqsUtil.generateMessage(fileName, bucketName, date));

            LOGGER.info("Success");
            return "Success!";
        } catch (AWSException e) {
            return e.getMessage();
        }
    }

}
