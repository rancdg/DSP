import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

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
				loadToSQS(key);
				managerExists = true;
			}
			else
			loadToSQS(key);

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
		// If the bucket doesn't exist - will create it.
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

			if (!managerExists) {
				// Create a queue
				System.out.println("Creating a new SQS queue called MyQueue.\n");
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(
						"managerQueue1"); //maybe extrapolate
				managerQueue1Url = sqs.createQueue(createQueueRequest).getQueueUrl();
			}
			else {
				// Create a queue
				System.out.println("Creating a new SQS queue called MyQueue.\n");
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(
						"managerQueue1"); //maybe extrapolate
				managerQueue1Url = sqs.createQueue(createQueueRequest).getQueueUrl(); //TODO: use existing queue instead of creating one
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
			sqs.sendMessage(new SendMessageRequest(managerQueue1Url, URI));

		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}		
}
