package hudson.plugins.ec2;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import hudson.plugins.ec2.ssh.EC2UnixLauncher;
import org.kohsuke.stapler.DataBoundConstructor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import hudson.Extension;
import hudson.model.Hudson;
import hudson.model.Descriptor.FormException;
import hudson.slaves.NodeProperty;

public final class EC2SpotSlave extends EC2AbstractSlave {

    private final String spotInstanceRequestId;

    public EC2SpotSlave(String instanceId, String description, String remoteFS, int sshPort, int numExecutors, String labelString,
                        Mode mode, String initScript, String remoteAdmin, String rootCommandPrefix, String jvmopts, boolean stopOnTerminate,
                        String idleTerminationMinutes, String publicDNS, String privateDNS, List<EC2Tag> tags, String cloudName, String spotInstanceRequestId)
            throws FormException, IOException {

        this(description + " (" + instanceId + ")", instanceId, description, remoteFS, sshPort, numExecutors, labelString,
                Mode.NORMAL, initScript, Collections.<NodeProperty<?>>emptyList(), remoteAdmin, rootCommandPrefix, jvmopts, stopOnTerminate,
                idleTerminationMinutes, publicDNS, privateDNS, tags, cloudName, false, spotInstanceRequestId);
    }

    public EC2SpotSlave(String instanceId, String description, String remoteFS, int sshPort, int numExecutors, String labelString,
                        Mode mode, String initScript, String remoteAdmin, String rootCommandPrefix, String jvmopts, boolean stopOnTerminate,
                        String idleTerminationMinutes, String publicDNS, String privateDNS, List<EC2Tag> tags, String cloudName, boolean usePrivateDnsName,
                        String spotInstanceRequestId)
            throws FormException, IOException {

        this(description + " (" + instanceId + ")", instanceId, description, remoteFS, sshPort, numExecutors, labelString,
                Mode.NORMAL, initScript, Collections.<NodeProperty<?>>emptyList(), remoteAdmin, rootCommandPrefix, jvmopts, stopOnTerminate,
                idleTerminationMinutes, publicDNS, privateDNS, tags, cloudName, usePrivateDnsName, spotInstanceRequestId);
    }

    @DataBoundConstructor
    public EC2SpotSlave(String name, String instanceId, String description, String remoteFS, int sshPort, int numExecutors,
                        String labelString, Mode mode, String initScript, List<? extends NodeProperty<?>> nodeProperties,
                        String remoteAdmin, String rootCommandPrefix, String jvmopts, boolean stopOnTerminate, String idleTerminationMinutes,
                        String publicDNS, String privateDNS, List<EC2Tag> tags, String cloudName, boolean usePrivateDnsName, String spotInstanceRequestId)
            throws FormException, IOException {

        super(name, instanceId, description, remoteFS, sshPort, numExecutors, mode, labelString, new EC2UnixLauncher(),
                new EC2RetentionStrategy(idleTerminationMinutes), initScript, nodeProperties, remoteAdmin, rootCommandPrefix,
                jvmopts, stopOnTerminate, idleTerminationMinutes, tags, cloudName, usePrivateDnsName);

        this.publicDNS = publicDNS;
        this.privateDNS = privateDNS;
        this.name = name;
        this.spotInstanceRequestId = spotInstanceRequestId;
    }

    /**
     * Cancel the spot request for the instance.
     * Terminate the instance if it is up.
     * Remove the slave from Jenkins.
     */
    @Override
    public void terminate() {
        try {
            Hudson.getInstance().removeNode(this);

            if (!isAlive(true)) {
                /* The node has been killed externally, so we've nothing to do here */
                LOGGER.info("EC2 instance already terminated: " + getInstanceId());
            } else {
                AmazonEC2 ec2 = cloud.connect();
                TerminateInstancesRequest request = new TerminateInstancesRequest(Collections.singletonList(getInstanceId()));
                ec2.terminateInstances(request);
                LOGGER.info("Terminated EC2 instance (terminated): " + getInstanceId());
            }
        } catch (AmazonClientException e) {
            LOGGER.log(Level.WARNING, "Failed to terminate EC2 instance: " + getInstanceId(), e);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to terminate EC2 instance: " + getInstanceId(), e);
        }
    }

    /**
     * Retrieve the SpotRequest for a requestId
     *
     * @param requestId
     * @return SpotInstanceRequest object for the requestId, or null
     */
    private SpotInstanceRequest getSpotRequest(String spotRequestId) {
        AmazonEC2 ec2 = cloud.connect();

        DescribeSpotInstanceRequestsRequest dsirRequest = new DescribeSpotInstanceRequestsRequest().withSpotInstanceRequestIds(spotRequestId);
        DescribeSpotInstanceRequestsResult dsirResult = ec2.describeSpotInstanceRequests(dsirRequest);
        List<SpotInstanceRequest> siRequests = dsirResult.getSpotInstanceRequests();
        if (siRequests.size() <= 0) return null;
        return siRequests.get(0);
    }

    /**
     * Accessor for the spotInstanceRequestId
     */
    public String getSpotInstanceRequestId() {
        return spotInstanceRequestId;
    }

    @Override
    public String getInstanceId() {
        if (instanceId == null || instanceId.equals("")) {
            SpotInstanceRequest sr = getSpotRequest(spotInstanceRequestId);
            if (sr != null)
                instanceId = sr.getInstanceId();
        }
        return instanceId;
    }

    @Extension
    public static final class DescriptorImpl extends EC2AbstractSlave.DescriptorImpl {

        @Override
        public String getDisplayName() {
            return Messages.EC2SpotSlave_AmazonEC2SpotInstance();
        }
    }

    private static final Logger LOGGER = Logger.getLogger(EC2SpotSlave.class.getName());

    @Override
    public String getEc2Type() {
        String spotMaxBidPrice = this.getSpotRequest(spotInstanceRequestId).getSpotPrice();
        return Messages.EC2SpotSlave_Spot1() + spotMaxBidPrice.substring(0, spotMaxBidPrice.length() - 3) + Messages.EC2SpotSlave_Spot2();
    }


}
