package io.jenkins.update_center;

import io.jenkins.update_center.util.Environment;

public class DefaultMavenRepositoryBuilder {

    private static String ARTIFACTORY_API_USERNAME = Environment.getString("ARTIFACTORY_USERNAME");
    private static String ARTIFACTORY_API_PASSWORD = Environment.getString("ARTIFACTORY_PASSWORD");

    private DefaultMavenRepositoryBuilder () {
        
    }

    private static BaseMavenRepository instance;
    
    public static synchronized BaseMavenRepository getInstance() {
        if (instance == null) {
            if (ARTIFACTORY_API_PASSWORD != null && ARTIFACTORY_API_USERNAME != null) {
                instance = new ArtifactoryRepositoryImpl(ARTIFACTORY_API_USERNAME, ARTIFACTORY_API_PASSWORD);
            } else {
                instance = new MavenCentralRepositoryImpl();
            }
        }
        return instance;
    }
}
