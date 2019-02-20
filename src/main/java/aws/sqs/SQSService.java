package aws.sqs;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQSService {

    private Logger LOGGER = LoggerFactory.getLogger(SQSService.class);

    private String SQS_URL = System.getenv("QUEUE_URL");
    private Regions region = Regions.EU_WEST_1;

    private AmazonSQS sqs = initSQSClient(region);

    public void sendMessage(String message) throws AWSException {
        if (message == null || message.isEmpty()) {
            throw new AWSException("Can't send SQS message, because SQS message body is null or empty");
        }
        if (sqs == null) {
            throw new AWSException("Can't create SQS message, because SQS client is null");
        }

        LOGGER.debug("Sending msg to SQS [{}]", message);
        SendMessageResult smr;

        try {
            smr = sqs.sendMessage(new SendMessageRequest()
                    .withMessageBody(message)
                    .withQueueUrl(SQS_URL));
        }catch (Exception e) {
            LOGGER.error("Error while creating SQS message: " + e);
            throw new AWSException("Error while creating SQS message: " + e);
        }

        LOGGER.info("Creation of SQS message succeeded with messageId [{}]", smr.getMessageId());
    }

    private AmazonSQS initSQSClient(Regions region) {
        AmazonSQS client = null;
        try {
            client = AmazonSQSClientBuilder.standard()
                    .withRegion(region)
                    .build();
        }catch (Exception e) {
            LOGGER.error("Error while initializing SQSClient: " + e);
        }
        return client;
    }
}
