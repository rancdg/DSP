import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import java.io.*;
import java.net.URL;
import java.util.List;
import javax.imageio.ImageIO;

public class Worker2
{

    public Worker2()
    {
    }

    public static void main(String args[])
        throws IOException
    {
        AWSCredentials credentials = new PropertiesCredentials(Worker2.getResourceAsStream("AwsCredentials.properties"));
        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(Worker2.getResourceAsStream("AwsCredentials.properties")));
        String newImgTask = "https://sqs.us-east-1.amazonaws.com/558898978765/newImgTask";
        String doneImgTask = "https://sqs.us-east-1.amazonaws.com/558898978765/doneImgTask";
        String oldUrl = "";
        String newUrl = "";
        String messageRecieptHandle = "";
        Boolean flag = Boolean.valueOf(true);
        int k = 1;
        AmazonS3 s3 = new AmazonS3Client(credentials);
        while(flag.booleanValue()) 
        {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(newImgTask);
            List messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            if(messages.isEmpty())
            {
                flag = Boolean.valueOf(false);
            } else
            {
                Message currentMsg = (Message)messages.get(0);
                oldUrl = currentMsg.getBody();
                messageRecieptHandle = currentMsg.getReceiptHandle();
                System.out.println(currentMsg.getBody());
                sqs.deleteMessage(new DeleteMessageRequest(newImgTask, messageRecieptHandle));
                TextOverlay overlay = new TextOverlay(currentMsg.getBody());
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                ImageIO.write(overlay.getBuffer(), "png", os);
                byte buffer[] = os.toByteArray();
                java.io.InputStream is = new ByteArrayInputStream(buffer);
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(buffer.length);
                meta.setContentType("jpg");
                String newName = (new StringBuilder("Instance--")).append(Integer.toString(k)).toString();
                s3.putObject((new PutObjectRequest("liorarnonImg", newName, is, meta)).withCannedAcl(CannedAccessControlList.PublicRead));
                System.out.println((new StringBuilder("the new name:")).append(newName).toString());
                newUrl = (new StringBuilder("https://s3.amazonaws.com/liorarnonImg/")).append(newName).toString();
                sqs.sendMessage(new SendMessageRequest(doneImgTask, (new StringBuilder(String.valueOf(oldUrl))).append(",").append(newUrl).toString()));
                k++;
            }
        }
        System.out.println("Turn  Manager Again");
    }

    public static String getInstanceOfWorker()
        throws IOException
    {
        URL instanceOfWorker = new URL("http://169.254.169.254/latest/meta-data/instance-id");
        BufferedReader br = new BufferedReader(new InputStreamReader(instanceOfWorker.openStream()));
        return br.readLine();
    }

    public static String readFile(String fileName)
        throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String s;
        StringBuilder sb = new StringBuilder();
        for(String line = br.readLine(); line != null; line = br.readLine())
        {
            sb.append(line);
            sb.append("\n");
        }

        s = sb.toString();
        br.close();
        return s;
        Exception exception;
        exception;
        br.close();
        throw exception;
    }
}