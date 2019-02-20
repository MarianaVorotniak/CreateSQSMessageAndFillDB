package aws.sqs.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(SqsUtil.class);

    public static String generateMessage(String fileName, String bucketName, String date) {
        if (fileName == null || bucketName == null || date == null) {
            LOGGER.error("fileName[{}], bucketName[{}], date[{}]", fileName, bucketName, date);
        }

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        SQSMessage msg = createObject(fileName, bucketName, date);
        String jsonInString = null;

        try {
            jsonInString = ow.writeValueAsString(msg);
            LOGGER.info("SQS message successfully created [{}]", jsonInString);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error occurred while creating SQS message for file [{}] in bucket [{}] in JSON format", fileName, bucketName);
        }

        return jsonInString;
    }

    private static SQSMessage createObject(String fileName, String bucketName, String date) {
        SQSMessage sqsMessage = new SQSMessage();

        sqsMessage.setFileName(fileName);
        sqsMessage.setBucketName(bucketName);
        sqsMessage.setDate(date);

        return sqsMessage;
    }

    public static class SQSMessage {
        private String fileName;
        private String bucketName;
        private String date;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getFileName() {
            return fileName;
        }

        public String getBucketName() {
            return bucketName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }
    }
}
