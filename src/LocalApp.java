import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

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
	int n;


	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{

		if (args.length == 4 && args[4].equals("terminate")) {
			//TODO: terminate();
		}

		else if (args.length == 3) {
			
			inputFile = args[0];
			outputFile = args[1];
			try {
				int n = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) {
				System.err.println("Argument" + args[3] + " must be an integer.");
				System.exit(1);
			}
			loadUrlsS3();
		}
		
		else {		
			System.err.println("Invalid arguments");
			System.exit(1);
		}
		
		
		//loadSQS();
		//requestManager();

	}

	private static void loadUrlsS3() throws FileNotFoundException,
	IOException, InterruptedException{

		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");
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

	}
}
