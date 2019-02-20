import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import exceptions.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import services.DBService;

public class CreateMessage implements RequestHandler<S3Event, String> {

    private static Logger LOGGER = LoggerFactory.getLogger(CreateMessage.class);

    private Regions REGION = Regions.EU_WEST_1;

    private DBService dbService;

    public String handleRequest(S3Event s3Event, Context context) {
        try {
            String message = getS3BucketFileName(s3Event) + "\n" + getS3BucketName(s3Event);
            dbService.createDBRecord(s3Event, REGION);
            createSQSMessage(message);

            return "Success!";
        } catch (AWSException e) {
            return e.getMessage();
        }
    }

}
