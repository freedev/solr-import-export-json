package it.damore.solr.importexport;

import java.io.IOException;
import java.net.http.HttpRequest;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

/**
 * https://github.com/freedev/solr-import-export-json/issues/43
 * https://stackoverflow.com/questions/2014700/preemptive-basic-authentication-with-apache-httpclient-4/11868040#11868040
 * 
 * @author i.baricic
 * @since 02.10.2020
 */
public class PreemptiveAuthInterceptor implements HttpRequestInterceptor {
    
    @Override
    public void process(org.apache.http.HttpRequest request, HttpContext context) throws HttpException, IOException {
        AuthState authState = (AuthState) context.getAttribute(HttpClientContext.TARGET_AUTH_STATE);
        if (authState.getAuthScheme() == null) {
            CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(HttpClientContext.CREDS_PROVIDER);
            HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
            AuthScope authScope = new AuthScope(targetHost.getHostName(), targetHost.getPort());
            Credentials creds = credsProvider.getCredentials(authScope);
            authState.update(new BasicScheme(), creds);
        }
    }

}
