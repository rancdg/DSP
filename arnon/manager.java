import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import java.io.*;
import java.util.*;
import org.apache.commons.codec.binary.Base64;

public class Manager
{

    public Manager()
    {
    }

    public static void main(String args[])
        throws IOException, InterruptedException
    {
        sqs = new AmazonSQSClient(new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties")));
        AWSCredentials credentials = new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties"));
        s3 = new AmazonS3Client(credentials);
        ec2 = new AmazonEC2Client(credentials);
        newTask = "https://sqs.us-east-1.amazonaws.com/558898978765/NewTask";
        newImgTask = "https://sqs.us-east-1.amazonaws.com/558898978765/newImgTask";
        doneImgTask = "https://sqs.us-east-1.amazonaws.com/558898978765/doneImgTask";
        doneTask = "https://sqs.us-east-1.amazonaws.com/558898978765/doneTask";
        String ImageListKey = "";
        int countImgs = 0;
        boolean check = true;
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newTask);
        List messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        List TempMessages = new ArrayList();
        while(check) 
        {
            if(messages.size() > 0)
            {
                String message = ((Message)messages.get(0)).getBody();
                String arr[] = message.split("numOfWorkers:");
                message = arr[0];
                Integer n = new Integer(arr[1]);
                arr = message.split("new task:");
                message = arr[0];
                ImageListKey = arr[1].substring(0, arr[1].length() - 1);
                S3Object object = s3.getObject(new GetObjectRequest("liorarnonImg", "image-urls.txt"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                do
                {
                    String line = reader.readLine();
                    if(line == null)
                        break;
                    sqs.sendMessage(new SendMessageRequest(newImgTask, line));
                    countImgs++;
                } while(true);
                System.out.println("Deleting a message.\n");
                String messageRecieptHandle = ((Message)messages.get(0)).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(sqs.getQueueUrl((new GetQueueUrlRequest()).withQueueName("NewTask")).getQueueUrl(), messageRecieptHandle));
                List instances = null;
                instances = createWorker((int)Math.ceil((float)countImgs / (float)n.intValue()));
                boolean flag = true;
                System.out.println("Turn Worker");
                int EndPics = countImgs;
                while(flag) 
                {
                    ReceiveMessageRequest receiveMessageRequest4 = new ReceiveMessageRequest(doneImgTask);
                    messages = sqs.receiveMessage(receiveMessageRequest4).getMessages();
                    if(messages.size() > 0)
                    {
                        TempMessages.add((Message)messages.get(0));
                        System.out.println(TempMessages.size());
                        String messageRecieptHandle2 = ((Message)messages.get(0)).getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(sqs.getQueueUrl((new GetQueueUrlRequest()).withQueueName("doneImgTask")).getQueueUrl(), messageRecieptHandle2));
                        EndPics--;
                    }
                    if(EndPics == 0)
                        flag = false;
                    else
                        Thread.sleep(20L);
                }
            }
            credentials = new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties"));
            s3 = new AmazonS3Client(credentials);
            sqs = new AmazonSQSClient(new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties")));
            CreateOutput(TempMessages, doneTask, credentials, s3, sqs);
            System.out.println("Turn LocalApp again");
            receiveMessageRequest = new ReceiveMessageRequest(newTask);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            if(messages.size() == 0)
                check = false;
        }
    }

    private static String getUserDataScript()
    {
        ArrayList lines = new ArrayList();
        lines.add("#! /bin/bash");
        lines.add("cd ~");
        lines.add("mkdir worker");
        lines.add("wget https://s3.amazonaws.com/liorarnonfiles/Worker2.jar >& workerWg.log");
        lines.add("java -jar Worker2.jar>& worker.log");
        lines.add("echo \"secretKey=Jd4yavw1Z+nbL8O5iLc5M///BsIVV20WU8zNBUME\">AwsCredentials.properties");
        lines.add("echo \"accessKey=AKIAIENSWVK7BX7TYDTA\">>AwsCredentials.properties");
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    static String join(Collection s, String delimiter)
    {
        StringBuilder builder = new StringBuilder();
        for(Iterator iter = s.iterator(); iter.hasNext(); builder.append(delimiter))
        {
            builder.append((String)iter.next());
            if(!iter.hasNext())
                break;
        }

        return builder.toString();
    }

    public static void CreateOutput(List TempMessages, String doneTask, AWSCredentials credentials, AmazonS3 s3, AmazonSQS sqs)
        throws IOException
    {
        credentials = new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties"));
        s3 = new AmazonS3Client(credentials);
        sqs = new AmazonSQSClient(new PropertiesCredentials(Manager.getResourceAsStream("AwsCredentials.properties")));
        File file = new File("try.html");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        String content = "<html><head><title> Arnon And Lior outpot </title></head>";
        content = (new StringBuilder(String.valueOf(content))).append("<body>\n  <table border=1>\n<tr>\n").toString();
        String currImg = "";
        int IndexNew = 0;
        int rowIndex = 0;
        String messageRecieptHandle = "";
        for(Iterator iterator = TempMessages.iterator(); iterator.hasNext();)
        {
            Message message = (Message)iterator.next();
            if(rowIndex == 5)
            {
                content = (new StringBuilder(String.valueOf(content))).append("</tr><tr>\n").toString();
                rowIndex = 0;
            }
            messageRecieptHandle = message.getReceiptHandle();
            IndexNew = message.getBody().indexOf(',');
            currImg = message.getBody().substring(IndexNew + 1, message.getBody().length());
            content = (new StringBuilder(String.valueOf(content))).append("<td  height =80 width =80>\n").append("line ").append(rowIndex).append("<img height=200 width=200 src=").append(currImg).append(">\n").append("</td>\n").toString();
            System.out.println((new StringBuilder(String.valueOf(currImg))).append("\n").toString());
            rowIndex++;
        }

        content = (new StringBuilder(String.valueOf(content))).append("</tr></table></body></html>").toString();
        bw.write(content);
        bw.close();
        s3.putObject((new PutObjectRequest("liorarnonoutput", "output.html", file)).withCannedAcl(CannedAccessControlList.PublicRead));
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-east-1.amazonaws.com/558898978765/doneTask", "https://s3.amazonaws.com/liorarnonoutput/output.html"));
    }

    private static List createWorker(int numW)
    {
        RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", Integer.valueOf(numW), Integer.valueOf(numW));
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.setUserData(getUserDataScript());
        request.setKeyName("arnonlior");
        RunInstancesResult runInstances = ec2.runInstances(request);
        return runInstances.getReservation().getInstances();
    }

    static AmazonSQS sqs;
    static AmazonS3 s3;
    static AmazonEC2 ec2;
    static String newTask;
    static String newImgTask;
    static String doneImgTask;
    static String doneTask;
}