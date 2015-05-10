import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.*;
import org.apache.commons.codec.binary.Base64;

public class LocalApp
{

    public LocalApp()
    {
    }

    public static void main(String args[])
        throws IOException, InterruptedException
    {
        List instances = null;
        AWSCredentials credentials = new PropertiesCredentials(LocalApp.getResourceAsStream("AwsCredentials.properties"));
        AmazonS3 s3 = new AmazonS3Client(credentials);
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(LocalApp.getResourceAsStream("AwsCredentials.properties")));
        String FileName = args[0];
        Integer n = new Integer(args[1]);
        File f = new File(FileName);
        s3.putObject(new PutObjectRequest("liorarnonImg", FileName, f));
        System.out.println("uploaded image-urls.txt");
        sqs.sendMessage(new SendMessageRequest("https://sqs.us-east-1.amazonaws.com/558898978765/NewTask", (new StringBuilder("new task:")).append(FileName).append(" numOfWorkers:").append(n).toString()));
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        boolean result = findOutIfMangerExsits(ec2);
        if(!result)
            try
            {
                RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", Integer.valueOf(1), Integer.valueOf(1));
                Thread.sleep(1000L);
                request.setInstanceType(InstanceType.T1Micro.toString());
                request.setUserData(getUserDataScript());
                request.setKeyName("arnonlior");
                RunInstancesResult runInstances = ec2.runInstances(request);
                List instances2 = runInstances.getReservation().getInstances();
                for(Iterator iterator = instances2.iterator(); iterator.hasNext(); System.out.println("Creating a new instance"))
                {
                    Instance instance = (Instance)iterator.next();
                    CreateTagsRequest createTagsRequest = new CreateTagsRequest();
                    createTagsRequest.withResources(new String[] {
                        instance.getInstanceId()
                    }).withTags(new Tag[] {
                        new Tag("1", "Manager")
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
        String outputUrl = "";
        boolean flag = true;
        while(flag) 
        {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("https://sqs.us-east-1.amazonaws.com/558898978765/doneTask");
            List messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            if(messages.size() == 1)
            {
                flag = false;
                outputUrl = ((Message)messages.get(0)).getBody();
                String messageRecieptHandle = ((Message)messages.get(0)).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(sqs.getQueueUrl((new GetQueueUrlRequest()).withQueueName("doneTask")).getQueueUrl(), messageRecieptHandle));
                URL website = new URL(outputUrl);
                java.nio.channels.ReadableByteChannel rbc = Channels.newChannel(website.openStream());
                FileOutputStream fos = new FileOutputStream("outputD.html");
                fos.getChannel().transferFrom(rbc, 0L, 0x7fffffffffffffffL);
                System.out.println("download file");
            } else
            {
                Thread.sleep(1000L);
            }
        }
        URL url = new URL(outputUrl);
        InputStream in = new BufferedInputStream(url.openStream());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte buf[] = new byte[1024];
        for(n = Integer.valueOf(0); -1 != (n = Integer.valueOf(in.read(buf))).intValue(); out.write(buf, 0, n.intValue()));
        out.close();
        in.close();
        byte response1[] = out.toByteArray();
        FileOutputStream fos = new FileOutputStream("C:/Users/dell/workspace/output.jpg");
        fos.write(response1);
        fos.close();
    }

    private static String getUserDataScript2()
    {
        System.out.println("trying  to add data");
        return new String(Base64.encodeBase64("#!/bin/bash \n wget https://s3.amazonaws.com/liorarnonfiles/ManagerJar.jar -O ManagerJar.jar \njava -jar ManagerJar.jar  https://sqs.us-east-1.amazonaws.com/558898978765/NewTask   https://sqs.us-east-1.amazonaws.com/558898978765/newImgTask  https://sqs.us-east-1.amazonaws.com/558898978765/doneImgTask https://sqs.us-east-1.amazonaws.com/558898978765/doneTask \n shutdown -h now ".getBytes()));
    }

    private static String getUserDataScript()
    {
        ArrayList lines = new ArrayList();
        lines.add("#! /bin/bash");
        lines.add("mkdir manager");
        lines.add("wget https://s3.amazonaws.com/liorarnonfiles/Manager.jar >& wget.log");
        lines.add("echo \"secretKey=85SWgANxhCxbbcSfATvDlOFRWnW71jQSNfxefluA\">AwsCredentials.properties");
        lines.add("echo \"accessKey=AKIAJE73PPVGF6A7YDBQ\">>AwsCredentials.properties");
        lines.add("java -jar Manager.jar>& Manager.log");
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

    public static boolean findOutIfMangerExsits(AmazonEC2 ec2)
    {
        String tagName = "Manager";
        String value = "1";
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
}