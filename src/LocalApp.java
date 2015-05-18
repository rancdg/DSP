import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
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
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class LocalApp{
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "eranfiles99";
	public static String propertiesFilePath = "cred.properties";
	public static String inputFile;
	public static String outputFile;
	public static String n;
	public static boolean managerExists = false;
	public static String appId= UUID.randomUUID().toString();
	public static String localAppQUrl = null;

	public static void main(String[] args) throws FileNotFoundException,
	IOException, InterruptedException{
		
		Credentials = new PropertiesCredentials(
				new FileInputStream(propertiesFilePath));
		System.out.println("Credentials created.");

		if (args.length == 4 && args[4].equals("terminate")) {
			System.out.println("Terminating");
			//TODO: terminate();
		}
		else if (args.length == 3) {

			//parse arguments
			inputFile = args[0];
			outputFile = args[1];
			n = args[2];
			
			
			//upload
			String key = uploadUrlsToS3(inputFile);
			String sqsURI = loadToSQS(key);
			AmazonEC2 ec2 = new AmazonEC2Client(Credentials);
			//managerExists = checkManagerExists(ec2);
			if (!managerExists){
				
				//if manager doesnt exist, load pics url's to manager's queue and start the manager				 
				try
	            {
	                RunInstancesRequest request = new RunInstancesRequest("ami-146e2a7c", 1, 1);
	                Thread.sleep(1000L);	                
	                request.setInstanceType(InstanceType.T2Micro.toString());
	                request.setUserData(returnUserData(sqsURI));
	                request.setKeyName("raneran");	               
	                RunInstancesResult runInstances = ec2.runInstances(request);
	                List instances2 = runInstances.getReservation().getInstances();
	                for(Iterator iterator = instances2.iterator(); iterator.hasNext(); System.out.println("Creating a new instance"))
	                {
	                    Instance instance = (Instance)iterator.next();
	                    CreateTagsRequest createTagsRequest = new CreateTagsRequest();
	                    createTagsRequest.withResources(new String[] {
	                        instance.getInstanceId()
	                    }).withTags(new Tag[] {
	                        new Tag("instanceType", "Manager")
	                    });
	                    ec2.createTags(createTagsRequest);
	                }

	            }
	            catch(AmazonServiceException ase)
	            {
	                System.out.println((new StringBuilder("Caught Exception: ")).append(ase.getMessage()).toString());
	                System.out.println((new StringBuilder("Reponse Status Code: ")).append(ase.getStatusCode()).toString());
	                System.out.println((new StringBuilder("Error Code: ")).append(ase.getErrorCode()).toString());
	                System.out.println((new StringBuilder("Request ID: ")).append(ase.getRequestId()).toString());
	            }
				
				//createManager(sqsURI);
				managerExists = true;
			}
			
			//wait for returning message:
			boolean Waiting = true;
			
			while (Waiting){
				System.out.println("Receiving messages from managerQueue.\n");
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(localAppQUrl);
				AmazonSQS appSQS = new AmazonSQSClient(Credentials);
				receiveMessageRequest.setWaitTimeSeconds(20);
				List<Message> messages = appSQS.receiveMessage(receiveMessageRequest).getMessages();
				if (messages.size() != 0){
					for (Message message : messages) {
						String messageBody = message.getBody();
						System.out.println("HTML address:     "+ messageBody);
					}
					Waiting=false;
				}
				else
					Thread.sleep(1000L);
					System.out.println("Waiting");
			}
			
		}

		else {		
			System.err.println("Invalid arguments");
			System.exit(1);
		}

	}

	private static boolean checkManagerExists(AmazonEC2 ec2) {
		String tagName = "Manager";
        String value = "instanceType";
        for(Iterator iterator = ec2.describeInstances().getReservations().iterator(); iterator.hasNext();)
        {
            Reservation reservation = (Reservation)iterator.next();
            for(Iterator iterator1 = reservation.getInstances().iterator(); iterator1.hasNext();)
            {
                Instance instance = (Instance)iterator1.next();
                if(instance.getState().getName().equals(InstanceStateName.Running.toString()))
                {
                    System.out.println(instance.getImageId());
                    System.out.println(instance.getInstanceId());
                    for(Iterator iterator2 = instance.getTags().iterator(); iterator2.hasNext();)
                    {
                        Tag tag = (Tag)iterator2.next();
                        if(tag.getKey().equals(value) && tag.getValue().equals(tagName))
                            return true;
                    }

                }
            }

        }

        return false;
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
		por.withCannedAcl(CannedAccessControlList.PublicRead);
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
				
		
		try {			

			//create own queue for returning messages
			// Create a queue
			System.out.println("Creating a new SQS queue called localAppQueue.\n");
			CreateQueueRequest localAppQueueRequest = new CreateQueueRequest(
					"localAppQueue-"+appId); //maybe extrapolate
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
			//create the message string
			String message = "appMessage " + localAppQUrl +" "+ URI+ " " + n + " "+ appId;
			SendMessageRequest sendRequest = new SendMessageRequest(managerQUrl, message);
			managerSQS.sendMessage(sendRequest);

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
	
	private static String returnUserData(String sqsURI) {
		
		StringBuilder builder = new StringBuilder();   	
    	builder.append("#! /bin/bash -xe");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("mkdir manager");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("wget https://s3.amazonaws.com/eranfiles99/Manager.jar >& wget.log");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("echo \"secretKey="+Credentials.getAWSSecretKey()+"\" > cred.properties");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("echo \"accessKey="+Credentials.getAWSAccessKeyId()+"\" >> cred.properties");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("java -jar Manager.jar "+sqsURI+" >& Manager.log");
    	builder.append(System.getProperty("line.separator"));
        
    	String userData = new String(Base64.encodeBase64(builder.toString().getBytes()));
		return userData;
	}
}
