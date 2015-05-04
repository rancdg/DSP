import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
public class LocalApp{
      public static PropertiesCredentials Credentials;
      public static AmazonS3 S3;
      public static String bucketName = "eranfiles2";
      public static String propertiesFilePath = "cred.properties";
      public static String fileToUploadPath = "README.md";
      public static void main(String[] args) throws FileNotFoundException,
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
            File f = new File(fileToUploadPath);
            PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(), f);
            // Upload the file
            S3.putObject(por);
            System.out.println("File uploaded.");
      }
}
