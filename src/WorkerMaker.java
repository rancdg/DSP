

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

public class WorkerMaker {

	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "eranfiles99";
	public static String propertiesFilePath = "cred.properties";
	public static String inputFile;
	public static String outputFile;
	public static String n;
	public static boolean managerExists = false;

	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{

		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");

		//create worker queue
		AmazonSQS workerSQS = new AmazonSQSClient(Credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		workerSQS.setRegion(usEast1);

		System.out.println("Creating a new SQS queue called workerQueue.\n");
		CreateQueueRequest workerQueueRequest = new CreateQueueRequest(
				"workerQueue"); //maybe extrapolate
		String workerQUrl = workerSQS.createQueue(workerQueueRequest).getQueueUrl();
		
		System.out.println("Sending a message to managerQueue1.\n");
		String message = "http://25.media.tumblr.com/tumblr_mcs2qmvPwB1qaxd6qo1_1280.gif";
		workerSQS.sendMessage(new SendMessageRequest(workerQUrl, message));

		createWorker(workerQUrl);
		//requestManager();
	}

	private static void createWorker(String SqsURI) throws FileNotFoundException, IOException, InterruptedException{
		String[] args = {SqsURI};
		Worker.main(args);
		
	}
	
	private static String uploadUrlsToS3(String inputFile) throws FileNotFoundException,
	IOException, InterruptedException{

		S3 = new AmazonS3Client(Credentials);
		System.out.println("AmazonS3Client created.");
		// If the bucket doesn't exist - will create it.
		// Notice - this will create it in the default region :Region.US_Standard
		if (!S3.doesBucketExist(bucketName)) {
			S3.createBucket(bucketName);
		}
		else 
			System.out.println("Bucket exist.");
		File f = new File(inputFile);
		PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(), f);
		// Upload the file
		S3.putObject(por);
		System.out.println("File uploaded.");
		return f.getName();

	}

	private static String loadToSQS(String key) {

		//create the manager SQS (new manager)
		AmazonSQS managerSQS = new AmazonSQSClient(Credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		managerSQS.setRegion(usEast1);

		//create the local app SQS
		AmazonSQS localAppSQS = new AmazonSQSClient(Credentials);
		localAppSQS.setRegion(usEast1);

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon SQS");
		System.out.println("===========================================\n");
		String managerQUrl = null;
		String localAppQUrl = null;		

		try {			

			//create own queue for returning messages
			// Create a queue
			System.out.println("Creating a new SQS queue called localAppQueue.\n");
			CreateQueueRequest localAppQueueRequest = new CreateQueueRequest(
					"localAppQueue"); //maybe extrapolate
			localAppQUrl = localAppSQS.createQueue(localAppQueueRequest).getQueueUrl();

			if (!managerExists) {
				// Create a queue
				System.out.println("Creating a new SQS queue called managerQueue.\n");
				CreateQueueRequest managerQueueRequest = new CreateQueueRequest(
						"managerQueue"); //maybe extrapolate
				managerQUrl = managerSQS.createQueue(managerQueueRequest).getQueueUrl();
			}
			else {
				// Create a queue
				System.out.println("Creating a new SQS queue called managerQueue.\n");
				CreateQueueRequest managerQueueRequest = new CreateQueueRequest(
						"managerQueue"); //maybe extrapolate
				managerQUrl = managerSQS.createQueue(managerQueueRequest).getQueueUrl(); //TODO: use existing queue instead of creating one
			}

			//Send a message
			S3Object obj = S3.getObject(new GetObjectRequest(bucketName, key));
			String URI = obj.getObjectContent().getHttpRequest().getURI().toString();
			System.out.println("Sending a message to managerQueue1.\n");
			String message = localAppQUrl +" "+ URI+ " " + n;
			managerSQS.sendMessage(new SendMessageRequest(managerQUrl, message));

		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		return managerQUrl;
	}

	private static void createManager(String sqsURI) throws FileNotFoundException, IOException, InterruptedException {
		String[] args = new String[1];
		args[0] = sqsURI;
		Manager.main(args);
	}
}

