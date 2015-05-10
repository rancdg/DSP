import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Manager {

	public static String managerSqsURI;
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "eranfiles2";
	public static String propertiesFilePath = "cred.properties";
	public static AmazonSQS managerSQS;
	
	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{
		
		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");

		//Parse
		if (args.length>1){
			System.err.println("Invalid arguments");
			System.exit(1);
		}
		else
			managerSqsURI = args[0];
		
		//create SQS client
				managerSQS = new AmazonSQSClient(Credentials);
				S3 = new AmazonS3Client(Credentials);
				Region usEast1 = Region.getRegion(Regions.US_EAST_1);
				managerSQS.setRegion(usEast1);
				S3.setRegion(usEast1);
				
		recieveMessage();
		
		//delete queue:
		// Delete a queue
        try{
		System.out.println("Deleting the test queue.\n");
        managerSQS.deleteQueue(new DeleteQueueRequest(managerSqsURI));
    } catch (AmazonServiceException ase) {
        System.out.println("Caught an AmazonServiceException, which means your request made it " +
                "to Amazon SQS, but was rejected with an error response for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
        System.out.println("Caught an AmazonClientException, which means the client encountered " +
                "a serious internal problem while trying to communicate with SQS, such as not " +
                "being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }
	}

	private static void recieveMessage() {
		
		// Receive messages
        System.out.println("Receiving messages from managerQueue.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(managerSqsURI);
        
        List<Message> messages = managerSQS.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            
        	//get and parse message
        	String localAppURI = message.getBody().split(" ")[0];
        	String inputURI = message.getBody().split(" ")[1];
        	
        	//download photos URI list from S3
        	System.out.println("Downloading an object");
            //S3Object object = S3.getObject(new GetObjectRequest(bucketname, key)); //TODO: figure out how to use URLs to download instead of bucketname
            //System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            //displayTextInputStream(object.getObjectContent());
            
        	//for debugging:
        	System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();		
	}
	
	
	
}
