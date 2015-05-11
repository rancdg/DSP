import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Worker {

	public static String managerSqsURI;
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String propertiesFilePath = "cred.properties";
	public static AmazonSQS managerSQS;

	private static File imgResize(URL imgurl) throws IOException{
		BufferedImage originalImage = ImageIO.read(imgurl);
		BufferedImage newImage = new BufferedImage(50, 50, BufferedImage.TYPE_INT_RGB);

		Graphics2D g = newImage.createGraphics();
		g.drawImage(originalImage, 0, 0, 50, 50, null);
		File outputfile = new File("imgout");
		ImageIO.write(newImage, "gif", outputfile);
		g.dispose();
		return outputfile;
	}
	
	private static String uploadFileToS3(File uploadFile, String bucketName) throws FileNotFoundException,
	IOException, InterruptedException{
		
		
		// If the bucket doesn't exist - will create it.
		// Notice - this will create it in the default region :Region.US_Standard
		if (!S3.doesBucketExist("imgbucket")) {
			System.out.println("Bucket doesn't exist. Creating it.");
			S3.createBucket("imgbucket");
		}
		else
			System.out.println("Bucket exists.");
		PutObjectRequest por = new PutObjectRequest(bucketName, uploadFile.getName(), uploadFile);
		// Upload the file
		S3.putObject(por);
		System.out.println("File uploaded.");
		return uploadFile.getName();

	}

	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{
		
		Credentials = new PropertiesCredentials(
		new FileInputStream(propertiesFilePath));
		S3 = new AmazonS3Client(Credentials);
		System.out.println("AmazonS3Client created.");
		URL imgurl = new URL("http://25.media.tumblr.com/tumblr_mcs2qmvPwB1qaxd6qo1_1280.gif");
		File img = imgResize(imgurl);
		uploadFileToS3(img, "imgTestBucket");
		
		
		/*
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
		}*/
	}

	private static void recieveMessage() throws IOException {

		// Receive messages
		System.out.println("Receiving messages from managerQueue.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(managerSqsURI);

		List<Message> messages = managerSQS.receiveMessage(receiveMessageRequest).getMessages();
		for (Message message : messages) {

			//get and parse message
			String[] messageBody = message.getBody().split(" ");
			String localAppURI = messageBody[0];
			String inputURI = messageBody[1];
			int n = Integer.parseInt(messageBody[3]);


			//download photos URI list from S3
			System.out.println("Downloading an object");
			AmazonS3URI S3URI = new AmazonS3URI(inputURI);
			S3Object object = S3.getObject(new GetObjectRequest(S3URI.getBucket(), S3URI.getKey())); //TODO: figure out how to use URLs to download instead of bucketname
			System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			String line = reader.readLine();

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
