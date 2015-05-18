import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Manager {

	public static String managerSqsURI;
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "eranfiles99";
	public static String propertiesFilePath = "cred.properties";
	public static AmazonSQS managerSQS;
	public static AmazonSQS taskSQS;
	public static AmazonSQS localAppSQS;
	public static String taskQUrl = null;
	public static int activeWorkersAmount = 0;
	public static Map<String,List<String>> urlTable = new HashMap<>(); //<appId , urls>
	public static Map<String,Integer> schedulingTable = new HashMap<>(); //<appId , remaining tasks>
	public static Map<String,String> appId_SQS_lookupTable = new HashMap<>(); //<appId , appSQSUrl>

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

		//create a task SQS
		try {
			taskSQS = new AmazonSQSClient(Credentials);
			CreateQueueRequest taskQueueRequest = new CreateQueueRequest(
					"taskQueue"); //maybe extrapolate
			taskQUrl = taskSQS.createQueue(taskQueueRequest).getQueueUrl();

		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

		recieveMessage(managerSqsURI);

		//delete queue:
		// Delete a queue
		deleteQueue(managerSqsURI);
	}

	private static void recieveMessage(String managerSqsURI) throws IOException, InterruptedException {

		// Receive messages
		System.out.println("Receiving messages from managerQueue.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(managerSqsURI);
		List<Message> messages = null;
		do {
			messages = managerSQS.receiveMessage(
					receiveMessageRequest).getMessages();
			for (Message message : messages) {

				//get and parse message
				String[] messageBody = message.getBody().split(" ");
				int n = 1;
				String messageType = null;
				String appId = null;
				String localAppURI = null;
				String inputURI = null;
				messageType = messageBody[0];

				//deal with an app message
				if (messageType.equals("appMessage")) {
					localAppURI = messageBody[1];
					inputURI = messageBody[2];

					if (messageBody.length >= 4)
						n = Integer.parseInt(messageBody[3]);
					if (messageBody.length >= 5)
						appId = messageBody[4];

					//add to lookup
					appId_SQS_lookupTable.put(appId, localAppURI);

					//download photos URI list from S3
					System.out.println("Downloading an object");
					AmazonS3URI S3URI = new AmazonS3URI(inputURI);
					S3Object object = S3.getObject(new GetObjectRequest(S3URI
							.getBucket(), S3URI.getKey()));
					System.out.println("Content-Type: "
							+ object.getObjectMetadata().getContentType());
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(object.getObjectContent()));

					//count jobs and insert them to task queue:
					int countJobs = 0;
					String line = reader.readLine();
					System.out.println("building and counting tasks for appId-"
							+ appId);
					List<SendMessageBatchRequestEntry> messageEntries = new ArrayList<>();
					while (line != null) {
						//build the message batch
						String jobMessage = line + " " + appId;
						messageEntries.add(new SendMessageBatchRequestEntry(
								String.valueOf(countJobs), jobMessage));
						countJobs++;
						line = reader.readLine();
					}
					SendMessageBatchRequest sendRequest = new SendMessageBatchRequest(
							taskQUrl, messageEntries);
					taskSQS.sendMessageBatch(sendRequest);
					System.out.println("counted " + countJobs + " jobs");
					
					//initialize urlTable for this app
					urlTable.put(appId, new ArrayList<String>());
					schedulingTable.put(appId, countJobs);

					//figure out how many workers needed
					int neededWorkers = 0;
					if (countJobs % n != 0) {
						//1 more worker for the jobs/n remaining
						neededWorkers = 1;
					}

					if (countJobs / n != 0)
						neededWorkers += countJobs / n - activeWorkersAmount;

					AmazonEC2 ec2 = new AmazonEC2Client(Credentials);

					//waking up workers
					System.out.println("waking up " + neededWorkers
							+ " needed workers");
					for (int i = 0; i < neededWorkers; i++) {

											try
								            {
								                RunInstancesRequest request = new RunInstancesRequest("ami-146e2a7c", 1, 1);
								                Thread.sleep(1000L);
								                request.setInstanceType(InstanceType.T2Micro.toString());
								                request.setUserData(returnUserData());
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
								                        new Tag("instanceType", "Worker")
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

//						String[] args = new String[2];
//						args[0] = taskQUrl;
//						args[1] = managerSqsURI;
//						Worker.main(args);

						activeWorkersAmount++;

					}
				} else if (messageType.equals("workerMessage")) {
					//parse
					String newImgUrl = messageBody[1];
					String origImgUrl = messageBody[2];
					String returnedAppId = messageBody[3];

					System.out.println("processing a worker message");
					//add to list
					List<String> urlList = urlTable.get(returnedAppId);
					String doubleUrl = newImgUrl + " " + origImgUrl;
					urlList.add(doubleUrl);

					//generate HTML if needed
					if (urlList.size() == schedulingTable.get(returnedAppId)) {
						System.out.println("generating an html");
						String HTMLkey = generateHTML(urlList, returnedAppId);
						S3Object obj = S3.getObject(new GetObjectRequest(bucketName, HTMLkey));
						String HTMLUrl = obj.getObjectContent().getHttpRequest().getURI().toString();
						
						System.out.println("generated this url: " + HTMLUrl);
						localAppSQS = new AmazonSQSClient(Credentials);
						Region usEast1 = Region.getRegion(Regions.US_EAST_1);
						localAppSQS.setRegion(usEast1);
						SendMessageRequest sendRequest3 = new SendMessageRequest(appId_SQS_lookupTable.get(returnedAppId), HTMLUrl);
						localAppSQS.sendMessage(sendRequest3);
					}
				}

				//for debugging:
				System.out.println("  Message");
				System.out.println("    MessageId:     "
						+ message.getMessageId());
				System.out.println("    ReceiptHandle: "
						+ message.getReceiptHandle());
				System.out.println("    MD5OfBody:     "
						+ message.getMD5OfBody());
				System.out.println("    Body:          " + message.getBody());
				//System.out.println("    appId:          " + appId);
				//System.out.println("    messageType:          " + messageType);

				for (Entry<String, String> entry : message.getAttributes()
						.entrySet()) {
					System.out.println("  Attribute");
					System.out.println("    Name:  " + entry.getKey());
					System.out.println("    Value: " + entry.getValue());
				}
				
				//Delete message
				DeleteMessageRequest del = new DeleteMessageRequest(managerSqsURI, message.getReceiptHandle());
				managerSQS.deleteMessage(del);
				
			}
		} while (messages.size() != 0);
		System.out.println();		
	}
	
	//helper fo html creating
	private static String generateHTML(List<String> urlList , String appId) throws FileNotFoundException, IOException, InterruptedException {
		
		String key = null;
		try {
 
			key = appId + ".html";
			File file = new File(key);
			
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
 
			System.out.println("Done");
 
		
		for (String urls : urlList){
			
			String newImgUrl = urls.split(" ")[0];
			String origImgUrl = urls.split(" ")[1];
			bw.write("<a href=\""+origImgUrl+"\" target=\"_blank\"><img src=\""+newImgUrl+"\" alt=\"Resized JPEG graphic\" title=\"Click to view\" border=\"2\" width=\"50\" height=\"50\" hspace=\"10\" /></a>");			
		}
		bw.close();
		
	} catch (IOException e) {
		e.printStackTrace();
	}
		return uploadToS3(key); //return HTML URL
	}

	private static String uploadToS3(String key) throws FileNotFoundException,
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
			File f = new File(key);
			PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(), f);
			// Upload the file
			por.withCannedAcl(CannedAccessControlList.PublicRead);
			S3.putObject(por);
			System.out.println("File uploaded.");
			return f.getName();

		}
	

	private static void deleteQueue(String managerSqsURI) {
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
	
private static String returnUserData() {
		
		StringBuilder builder = new StringBuilder();   	
    	builder.append("#! /bin/bash");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("mkdir manager");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("wget https://s3.amazonaws.com/eranfiles99/Worker.jar >& wget.log");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("echo \"secretKey="+Credentials.getAWSSecretKey()+"\" > cred.properties");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("echo \"accessKey="+Credentials.getAWSAccessKeyId()+"\" >> cred.properties");
    	builder.append(System.getProperty("line.separator"));
    	builder.append("java -jar Worker.jar "+taskQUrl +" "+ managerSqsURI + " >& Worker.log");
    	builder.append(System.getProperty("line.separator"));
        
    	String userData = new String(Base64.encodeBase64(builder.toString().getBytes()));
		return userData;
	}
}
