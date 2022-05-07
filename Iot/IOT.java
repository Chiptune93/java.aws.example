import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iot.IotClient;
import software.amazon.awssdk.services.iot.IotClientBuilder;
import software.amazon.awssdk.services.iot.model.CreateJobRequest;
import software.amazon.awssdk.services.iot.model.CreateJobRequest.Builder;
import software.amazon.awssdk.services.iot.model.CreateJobResponse;
import software.amazon.awssdk.services.iot.model.GroupNameAndArn;
import software.amazon.awssdk.services.iot.model.JobExecutionsRolloutConfig;
import software.amazon.awssdk.services.iot.model.ListThingGroupsResponse;
import software.amazon.awssdk.services.iot.model.PresignedUrlConfig;
import software.amazon.awssdk.services.iot.model.TargetSelection;
import software.amazon.awssdk.services.iot.model.TimeoutConfig;

public class IOT {

    // IOT Client
    private IotClient iotClient;

    // AWS ARN
    private String iamArn;

    // IOT ARN
    private String arn;

    /**
     * IOT Client Build
     * @param endpoint
     * @param region
     * @param accessKey
     * @param secretKey
     * @param env
     * @param iamArn
     * @param arn
     */
    public IotUtil(String endpoint, String region, String accessKey, String secretKey, String env, String iamArn,
            String arn) {
        // iot client build
        IotClientBuilder builder = IotClient.builder();
        try {
            // credential
            if ("local".equals(env) == true) {
                builder.credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            }

            if (endpoint.indexOf("http") <= -1)
                endpoint = "https://" + endpoint;

            // build client
            // builder.endpointOverride(uri);
            this.arn = arn;
            this.iamArn = iamArn;
            this.iotClient = builder.region(Region.AP_NORTHEAST_2).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get thing Group
     * 
     * @return
     */
    public List<HashMap<String, Object>> getThingGroup() {
        List<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
        ListThingGroupsResponse res = iotClient.listThingGroups();
        List<GroupNameAndArn> map = res.thingGroups();
        for (GroupNameAndArn g : map) {
            String groupName = g.groupName();
            String groupArn = g.groupArn();
            HashMap<String, Object> thingGroup = new HashMap<String, Object>();
            thingGroup.put("groupName", groupName);
            thingGroup.put("groupArn", groupArn);
            result.add(thingGroup);
        }
        return result;
    }

    /**
     * create job and request to aws
     * 
     * @param jobDocument
     * @param info
     * @return
     */
    public HashMap<String, Object> crateJob(String jobDocument, HashMap<String, Object> info) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        // create job request
        Builder jobBuilder = CreateJobRequest.builder();
        CreateJobRequest createJobRequest = null;

        try {
            // job id
            UUID uid = UUID.randomUUID();
            jobBuilder.jobId(info.getStr("otaName", ""));
            // description
            jobBuilder.description("description");
            // jobDocument
            jobBuilder.document(jobDocument);
            jobBuilder.presignedUrlConfig(PresignedUrlConfig.builder()
                    .roleArn(iamArn + ":role/service-role/{role-name}").expiresInSec(3600L).build());
            jobBuilder.jobExecutionsRolloutConfig(JobExecutionsRolloutConfig.builder().maximumPerMinute(1000).build());
            jobBuilder.timeoutConfig(TimeoutConfig.builder().inProgressTimeoutInMinutes(10L).build());
            // target device
            List<String> tragets = new ArrayList<String>();
            if (info.getStr("thinGrup").equals("") || info.getStr("thinGrup").equals(null)) {
                // if device id added repeatly
                for (HashMap<String, Object> map : info.get("deviceList")) {
                    tragets.add(arn + ":thing/" + map.getStr("deviceId"));
                }
            } else {
                // if target group added
                tragets.add(info.getStr("thinGrup"));
            }
            jobBuilder.targets(tragets);
            jobBuilder.targetSelection(TargetSelection.SNAPSHOT);

            createJobRequest = jobBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // create job
            CreateJobResponse response = iotClient.createJob(createJobRequest);
            // response.jobArn(); // excution job arn
            // response.jobId(); // execution job id
            // response.description(); // execution job description
            result.put("status", "success");
            result.put("jobArn", response.jobArn());
            result.put("jobId", response.jobId());
            result.put("description", response.description());
        } catch (Exception e) {
            result.put("status", "fail");
            e.printStackTrace();
        }

        return result;
    }
}
