import java.io.IOException;
import java.util.Optional;
import java.net.URI;
import java.util.Date;
import java.util.logging.Logger;
import java.lang.String;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import org.apache.hadoop.security.UserGroupInformation;


final class MyCredentialProvider implements AWSCredentialsProvider, Configurable {

    private Configuration configuration;
    private AWSCredentials credentials, iamUserCredentials;
    private URI uri;
    
    // Will be overridden after fetching current user
    private static String user_name = System.getProperty("user.name");
    private static Optional<UserGroupInformation> current_user;

    private static InstanceProfileCredentialsProvider creds;
    private static Credentials stsCredentials;
    private static String role_arn;
    private static String account_id;
    // Duration for which temporary credentials for assumed role will be valid.
    private static int session_duration = 3600;

    public MyCredentialProvider(URI uri, Configuration conf){
    	this.configuration = conf;
        this.uri = uri;

        UserGroupInformation currentUser = null;
	    try {
	      currentUser = UserGroupInformation.getCurrentUser();
	    } catch (IOException e) {}
        this.current_user = Optional.ofNullable(currentUser);
    }

    @Override
    public AWSCredentials getCredentials() {
        //Returning the credentials to EMRFS to make S3 API calls
        Boolean refreshCredentialsAsync = true;
        if (creds == null) {
            creds = new InstanceProfileCredentialsProvider
                (refreshCredentialsAsync);
        }

        if (stsCredentials == null ||
                (stsCredentials.getExpiration().getTime() - System.currentTimeMillis() < 60000)) {
            // Get the short user name from UGI
            if (current_user.isPresent()) {
	    		UserGroupInformation ugi = current_user.get();
	    		user_name = ugi.getShortUserName();
	    		// System.out.println("Fetched user: " + user_name);
	    	}
            // Use Instance profile to assume mapped role
            iamUserCredentials = creds.getCredentials();
        	AWSSecurityTokenServiceClient stsClient = new
                        AWSSecurityTokenServiceClient(iamUserCredentials);
            // Get accound ID by calling STS Get-Caller-Identity
        	GetCallerIdentityResult callerIdentity = stsClient.getCallerIdentity(new GetCallerIdentityRequest());
        	account_id = callerIdentity.getAccount();
        	String arn = callerIdentity.getArn();
        	String[] parts = arn.split("/");
        	String session_name = parts[2];
        	role_arn = "arn:aws:iam::" + account_id + ":role/emr_" + user_name;

        	System.out.println("Current user: " + user_name);
    		System.out.println("Mapped role: " + role_arn);

        	//Assuming the role to obtain temporary credentials
            AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
            .withRoleArn(role_arn)
            .withDurationSeconds(session_duration)
            .withRoleSessionName(session_name);
            AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);

            stsCredentials = assumeResult.getCredentials();
        }
        BasicSessionCredentials temporaryCredentials =
                new BasicSessionCredentials(
                        stsCredentials.getAccessKeyId(),
                        stsCredentials.getSecretAccessKey(),
                        stsCredentials.getSessionToken());
        credentials = temporaryCredentials;
        return credentials;
    }

    @Override
    public void refresh() {}

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}