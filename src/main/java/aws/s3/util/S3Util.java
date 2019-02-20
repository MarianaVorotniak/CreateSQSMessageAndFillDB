package aws.s3.util;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class S3Util {

    private static Logger LOGGER = LoggerFactory.getLogger(S3Util.class);

    public static String getS3BucketName(S3EventNotification.S3EventNotificationRecord record) throws AWSException {
        if (record == null) {
            LOGGER.error("S3EventNotificationRecord is null");
        }

        String bkt;
        try {
            bkt = record.getS3().getBucket().getName();
            LOGGER.info("Bucket name successfully received [{}]", bkt);
        }catch (Exception e) {
            LOGGER.error("Error while getting S3 Bucket name: " + e);
            throw new AWSException("Error while getting S3 Bucket name: " + e);
        }
        return bkt;
    }

    public static String getFileName(S3EventNotification.S3EventNotificationRecord record) throws AWSException {
        if (record == null) {
            LOGGER.error("S3EventNotificationRecord is null");
        }

        String key;
        try {
            key = record.getS3().getObject().getKey().replace('+', ' ');
            key = URLDecoder.decode(key, "UTF-8").split("/")[1];
        } catch (Exception e) {
            LOGGER.error("Error while getting file name: " + e);
            throw new AWSException("Error while getting file name: " + e);
        }
        LOGGER.info("File key successfully received [{}]", key);
        return key;
    }

    public static S3EventNotification.S3EventNotificationRecord getS3Record(S3Event s3Event) throws AWSException {
        if (s3Event == null) {
            LOGGER.error("S3 Event is null");
        }

        S3EventNotification.S3EventNotificationRecord record = null;
        try {
            if (!s3Event.getRecords().isEmpty()) {
                record = s3Event.getRecords().get(0);
            }
        } catch (Exception e) {
            LOGGER.error("Error while getting S3 Event Notification record");
            throw new AWSException("Error while getting S3 Event Notification record");
        }
        return record;
    }

    public static String getDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }
}
