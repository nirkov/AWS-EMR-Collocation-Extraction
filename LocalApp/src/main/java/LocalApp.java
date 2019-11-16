import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.hadoop.io.DoubleWritable;

import java.util.UUID;

public class LocalApp {
    public static void main(String[] args) {
        String subDir = UUID.randomUUID().toString();

        System.out.println(subDir);

        String jarsPath        = "s3n://hadoop-map-reduce-collocation-extraction/HadoopMapReduceJars/";
        String inputStepOne    = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
        String outputStepOne   = "s3n://hadoop-map-reduce-collocation-extraction/OutputStepOne/" + subDir;
        String outputStepTwo   = "s3n://hadoop-map-reduce-collocation-extraction/OutputStepTwo/" + subDir;
        String outputStepThree = "s3n://hadoop-map-reduce-collocation-extraction/OutputStepThree/" + subDir;
        final String minPmi    = args[0];
        final String relMinPmi = args[1];

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider).build();

        HadoopJarStepConfig hadoopJar_step_one = new HadoopJarStepConfig()
                .withJar(jarsPath + "MapReduceStepOne.jar")
                .withArgs(inputStepOne, outputStepOne);

        StepConfig stepOneConfig = new StepConfig()
                .withName("Step One")
                .withHadoopJarStep(hadoopJar_step_one)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJar_step_two = new HadoopJarStepConfig()
                .withJar(jarsPath + "MapReduceStepTwo.jar")
                .withArgs(outputStepOne, outputStepTwo);

        StepConfig stepTwoConfig = new StepConfig()
                .withName("Step Two")
                .withHadoopJarStep(hadoopJar_step_two)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJar_step_three = new HadoopJarStepConfig()
                .withJar(jarsPath + "MapReduceStepThree.jar")
                .withArgs(outputStepTwo, outputStepThree, minPmi, relMinPmi);

        StepConfig stepThreeConfig = new StepConfig()
                .withName("Step Three")
                .withHadoopJarStep(hadoopJar_step_three)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M1Xlarge.toString())
                .withHadoopVersion("2.2.0").withEc2KeyName("AWSElasticMapReduce")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map Reduce")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepOneConfig, stepTwoConfig, stepThreeConfig)
                .withLogUri("s3n://adoop-map-reduce-collocation-extraction/hadoop-map-reduce-log")
                .withJobFlowRole("EC2MapReduceRole")
                .withServiceRole("EMRMapReduceRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
