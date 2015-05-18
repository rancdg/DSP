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
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker {

	public static String managerSqsURI;
	public static String workerSqsURI;
	public static String bucketName = "eranfiles99";
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String propertiesFilePath = "cred.properties";
	public static AmazonSQS managerSQS;
	public static AmazonSQS workerSQS;

	/**
	 * 
	 * @param args - {String workerQueueURI, String managerQueueURI}
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{

		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		S3 = new AmazonS3Client(Credentials);
		System.out.println("AmazonS3Client created.");


		//Parse
		if (args.length>2){
			System.err.println("Invalid arguments");
			System.exit(1);
		}
		else{
			workerSqsURI = args[0];
			managerSqsURI = args[1]; 
		}

		//create SQS client
		workerSQS = new AmazonSQSClient(Credentials);
		managerSQS = new AmazonSQSClient(Credentials);
		S3 = new AmazonS3Client(Credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		workerSQS.setRegion(usEast1);
		managerSQS.setRegion(usEast1);
		S3.setRegion(usEast1);

		System.out.println("Receiving messages from workerQueue.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(workerSqsURI);
		List<Message> messages = null;
		do{
			messages = workerSQS.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {

				//get and parse message
				String[] messageBody = message.getBody().split(" ");
				String imgUrlStr = messageBody[0];
				String appId = messageBody[1];
				System.out.println("Incoming message: " + imgUrlStr);
				URL imgUrl = new URL(imgUrlStr);
				File newimage = imgResize(imgUrl);
				System.out.println(imgUrl.getFile());
				String key = uploadFileToS3(newimage ,  bucketName);


				//send response to manager
				S3Object obj = S3.getObject(new GetObjectRequest(bucketName, key));
				String URI = obj.getObjectContent().getHttpRequest().getURI().toString();
				System.out.println("Sending a message to managerQueue.\n");
				String returnMessage = "workerMessage "+ URI + " " + imgUrlStr + " " +appId;
				SendMessageRequest sendRequest2 = new SendMessageRequest(managerSqsURI, returnMessage);
				managerSQS.sendMessage(sendRequest2);


				//Delete message
				DeleteMessageRequest del = new DeleteMessageRequest(workerSqsURI, message.getReceiptHandle());
				workerSQS.deleteMessage(del);
			}

		}
		while (messages.size()!=0);
	}

	//A helper function which does the "work" - 
	//Resizes the image and returns a smaller image with a 'small_' prefix
	//Example: given the URL 'www.pictures.com/bla/pic/image1.gif'
	//The function returns a resized (50x50) file called 'small_image1.gif'
	private static File imgResize(URL imgurl) throws IOException{
		BufferedImage originalImage = ImageIO.read(imgurl);
		BufferedImage newImage = new BufferedImage(50, 50, BufferedImage.TYPE_INT_RGB);

		Graphics2D g = newImage.createGraphics();
		g.drawImage(originalImage, 0, 0, 50, 50, null);

		//Get the file name
		String tmp = imgurl.getFile().replace('/', '_');
		File outputfile = new File("small_" + tmp);

		ImageIO.write(newImage, getImageFormat(outputfile.getName()), outputfile);

		g.dispose();
		return outputfile;
	}

	private static String uploadFileToS3(File uploadFile , String bucketName) throws FileNotFoundException,
	IOException, InterruptedException{


		// If the bucket doesn't exist - will create it.
		// Notice - this will create it in the default region :Region.US_Standard
		if (!S3.doesBucketExist(bucketName)) {
			System.out.println("Bucket doesn't exist. Creating it.");
			S3.createBucket(bucketName);
		}
		else
			System.out.println("Bucket exists.");
		PutObjectRequest por = new PutObjectRequest(bucketName, uploadFile.getName(), uploadFile);
		// Upload the file
		por.withCannedAcl(CannedAccessControlList.PublicRead);
		S3.putObject(por);
		System.out.println("File uploaded.");
		return uploadFile.getName();

	}

	//A helper function to obtain the image format
	private static String getImageFormat(String img){
		String[] tmp = img.split("\\.");
		return tmp[tmp.length - 1];
	}







}
