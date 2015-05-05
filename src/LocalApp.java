import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApp{
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "eranfiles2";
	public static String propertiesFilePath = "cred.properties";
	public static String inputFile;
	public static String outputFile;
	public static int n;
	public static boolean managerExists = false;

	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{

		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");

		if (args.length == 4 && args[4].equals("terminate")) {
			//TODO: terminate();
		}

		else if (args.length == 3) {

			inputFile = args[0];
			outputFile = args[1];

			try {
				n = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				System.err.println("Argument" + args[2] + " must be an integer.");
				System.exit(1);
			}
			String key = uploadUrlsToS3();
			//TODO: check if manager exists
			if (!managerExists){
				//createManager();
				//loadToSQS();
				managerExists = true;
			}
			//else
			//loadToSQS();

		}

		else {		
			System.err.println("Invalid arguments");
			System.exit(1);
		}



		//requestManager();

	}

	private static String uploadUrlsToS3() throws FileNotFoundException,
	IOException, InterruptedException{

		S3 = new AmazonS3Client(Credentials);
		System.out.println("AmazonS3Client created.");
		// If the bucket doesnt exist - will create it.
		// Notice - this will create it in the default region :Region.US_Standard
		if (!S3.doesBucketExist(bucketName)) {
			S3.createBucket(bucketName);
		}
		System.out.println("Bucket exist.");
		File f = new File(inputFile);
		PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(), f);
		// Upload the file
		S3.putObject(por);
		System.out.println("File uploaded.");
		return f.getName();

	}

	private static void loadToSQS(String key) {

		AmazonSQS sqs = new AmazonSQSClient(Credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon SQS");
		System.out.println("===========================================\n");

		try {
			String managerQueue1Url;

			if (managerExists) {
				// Create a queue
				System.out.println("Creating a new SQS queue called MyQueue.\n");
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(
						"managerQueue1"); //maybe extrapolate
				managerQueue1Url = sqs.createQueue(createQueueRequest).getQueueUrl();
			}

			// List queues
			System.out.println("Listing all queues in your account.\n");
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				System.out.println("  QueueUrl: " + queueUrl);
			}
			System.out.println();

			// Send a message
			S3Object obj = S3.getObject(new GetObjectRequest(bucketName, key));
			String URI = obj.getObjectContent().getHttpRequest().getURI().toString();
			System.out.println("Sending a message to managerQueue1.\n");
			sqs.sendMessage(new SendMessageRequest(managerQueue1Url, "This is my message text."));

			// Receive messages
			System.out.println("Receiving messages from MyQueue.\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(managerQueue1Url);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
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

			// Delete a message
			System.out.println("Deleting a message.\n");
			String messageRecieptHandle = messages.get(0).getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(managerQueue1Url, messageRecieptHandle));

			// Delete a queue
			System.out.println("Deleting the test queue.\n");
			sqs.deleteQueue(new DeleteQueueRequest(managerQueue1Url));
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
}
