import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

public class S3Upload {

    // your bucket name
    private String bucketName;

    // s3 client
    private S3Client conn;

    /**
     * S3 Util connection object
     * @param accessKey
     * @param secretKey
     * @param region
     * @param profile
     * @param bucketName
     */
    public S3Util(String accessKey, String secretKey, String region, String profile, String bucketName) {
        // region
        Region region2 = Region.AP_NORTHEAST_2;
        if (profile.equals("local")) {
            // s3 connection build with keys
            AwsBasicCredentials credential = AwsBasicCredentials.create(accessKey, secretKey);
            this.conn = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(credential))
                    .region(region2)
                    .build();
            this.setBucketName(bucketName);
        } else {
            // s3 connection build without keys
            this.conn = S3Client.builder().region(region2).build();
        }
    }

    /**
     * get bucket list
     * 
     * @return
     */
    public ListBucketsResponse getBucketList() {
        return conn.listBuckets();
    }

    /**
     * create bucket list
     * 
     * @param bucketName
     * @return
     */
    public CreateBucketResponse createBucket(String bucketName) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();
        return conn.createBucket(createBucketRequest);
    }

    /**
     * upload file at bucket name
     * 
     * @param bucketName
     * @param fileName
     * @param fileData
     * @return
     * @throws FileNotFoundException
     */
    public MyMap fileUpload(String bucketName, String fileName, byte[] fileData) throws FileNotFoundException {

        String filePath = (fileName).replace(File.separatorChar, '/');
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(filePath)
                .build();

        ByteBuffer buffer = ByteBuffer.wrap(fileData);
        PutObjectResponse res = null;

        try {
            res = conn.putObject(putObjectRequest, software.amazon.awssdk.core.sync.RequestBody.fromByteBuffer(buffer));
        } catch (Exception e) {
            e.printStackTrace();
        }
        // result object setting
        MyMap result = new MyMap();
        result.put("bucketName", bucketName);
        result.put("filePath", filePath);
        return result;
    }

    /**
     * File Delete
     * 
     * @param fileName
     */
    public void fileDelete(String fileName) {
        String imgName = (fileName).replace(File.separatorChar, '/');
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(this.bucketName).key(imgName)
                .build();
        conn.deleteObject(deleteObjectRequest);
    }

    /**
     * File Check
     * 
     * @param bucketName
     * @param fileName
     * @return
     */
    public boolean fileExist(String bucketName, String fileName) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            HeadObjectResponse res = conn.headObject(headObjectRequest);
            if (res.hasMetadata()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * get File Url
     * 
     * @param bucketName
     * @param fileName
     * @param duration
     * @return
     * @throws Exception
     */
    public String getFileURL(String bucketName, String fileName, Duration duration) throws Exception {
        String result = "";
        try {
            S3Presigner presigner = S3Presigner.create();

            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(duration)
                    .getObjectRequest(objectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);

            // Upload content to the Amazon S3 bucket by using this URL
            result = presignedRequest.url().toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * get Real File
     * 
     * @param bucketName
     * @param fileName
     * @return
     * @throws IOException
     */
    public ResponseInputStream<GetObjectResponse> getSrcFile(String bucketName, String fileName) throws IOException {
        fileName = (fileName).replace(File.separatorChar, '/');
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();

        ResponseInputStream<GetObjectResponse> responseInputStream = conn.getObject(getObjectRequest);
        return responseInputStream;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}
