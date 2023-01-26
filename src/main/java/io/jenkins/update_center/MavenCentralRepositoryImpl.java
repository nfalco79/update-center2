package io.jenkins.update_center;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TeeOutputStream;

import com.alibaba.fastjson.JSON;

import io.jenkins.update_center.util.Environment;
import io.jenkins.update_center.util.HttpHelper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class MavenCentralRepositoryImpl extends BaseMavenRepository {
    private static final Logger LOGGER = Logger.getLogger(MavenCentralRepositoryImpl.class.getName());

    private static final String REPO_MAVEN_URL = "https://repo1.maven.org/maven2";
    private static final String SEARCH_MAVEN_URL = "https://search.maven.org";
    private static final String SEARCH_MAVEN_SEARCH_URL = SEARCH_MAVEN_URL + "/solrsearch/select?q=%s";
    private static final String SEARCH_MAVEN_DOWNLOAD_URL = SEARCH_MAVEN_URL + "/remotecontent?filepath=%s";

    private File cacheDirectory = new File(Environment.getString("ARTIFACTORY_CACHEDIR", "caches/artifactory"));

    private boolean initialized = false;

    private Map<String, JsonFile> files = new HashMap<>();
    private Set<ArtifactCoordinates> plugins;
    private Set<ArtifactCoordinates> wars;

    @Override
    protected Set<ArtifactCoordinates> listAllJenkinsWars(String groupId) throws IOException {
        ensureInitialized();
        return wars;
    }

    private static ArtifactCoordinates toGav(JsonFile f) {
        return new ArtifactCoordinates(f.g, f.a, f.latestVersion, f.p);
    }

    @Override
    public Collection<ArtifactCoordinates> listAllPlugins() throws IOException {
        ensureInitialized();
        return plugins;
    }

    private static class JsonFile {
        public String id;
        public String g;
        public String a;
        public String latestVersion;
        public String p;
        public long timestamp;
        public List<String> ec;
    }

    private static class JsonResponse {
        public List<JsonFile> docs;
    }

    private static class JsonResult {
        public JsonResponse response;
    }

    private Map<String, String> cache = new HashMap<>();

    private static final int CACHE_ENTRY_MAX_LENGTH = 1024 * 64;

    private void initialize() throws IOException {
        if (initialized) {
            throw new IllegalStateException("re-initialized");
        }
        LOGGER.log(Level.INFO, "Initializing " + this.getClass().getName());

        OkHttpClient client = new OkHttpClient.Builder().build();
        Request request = new Request.Builder().url(String.format(SEARCH_MAVEN_SEARCH_URL, "g:com.github.nfalco79"))
                .get().build();
        try (final ResponseBody body = HttpHelper.body(client.newCall(request).execute())) {
            final MediaType mediaType = body.contentType();
            JsonResult json = JSON.parseObject(body.byteStream(), mediaType == null ? StandardCharsets.UTF_8 : mediaType.charset(), JsonResult.class);
            json.response.docs.forEach(it -> this.files.put(it.g + ":" + it.a, it));
        }
        this.plugins = this.files.values().stream().filter(it -> it.ec.contains(".hpi")).map(MavenCentralRepositoryImpl::toGav).filter(Objects::nonNull).collect(Collectors.toSet());
        this.wars = this.files.values().stream().filter(it -> it.ec.contains(".war")).map(MavenCentralRepositoryImpl::toGav).collect(Collectors.toSet());
        LOGGER.log(Level.INFO, "Initialized " + this.getClass().getName());
    }

    private String hexToBase64(String hex) throws IOException {
        try {
            byte[] decodedHex = Hex.decodeHex(hex);
            return Base64.encodeBase64String(decodedHex);
        } catch (DecoderException e) {
            throw new IOException("failed to convert hex to base64", e);
        }
    }

    @Override
    public ArtifactMetadata getMetadata(MavenArtifact artifact) throws IOException {
        ensureInitialized();
        ArtifactMetadata ret = new ArtifactMetadata();
        final JsonFile jsonFile = files.get(artifact.artifact.groupId + ":" + artifact.artifact.artifactId);
        ret.timestamp = jsonFile.timestamp;

        // if artifact has been retrieved and cached than decorate medatata with additional info 
        File artifactFile = cachedFile(String.format(SEARCH_MAVEN_DOWNLOAD_URL, getUri(artifact.artifact)));
        if (artifactFile.isFile() && ret.size == 0) {
            ret.size = artifactFile.length();
            try (InputStream is = new FileInputStream(artifactFile)) {
                ret.sha256 = hexToBase64(DigestUtils.sha256Hex(is));
            }
            try (InputStream is = new FileInputStream(artifactFile)) {
                ret.sha1 = hexToBase64(DigestUtils.sha1Hex(is));
            }
        }
        return ret;
    }

    private void ensureInitialized() throws IOException {
        if (!initialized) {
            initialize();
            initialized = true;
        }
    }

    private String getUri(ArtifactCoordinates a) {
        String basename = a.artifactId + "-" + a.version;
        String filename = basename + "." + a.packaging;
        return a.groupId.replace(".", "/") + "/" + a.artifactId + "/" + a.version + "/" + filename;
    }

    @Override
    public Manifest getManifest(MavenArtifact artifact) throws IOException {
        return new Manifest(getZipFileEntry(artifact, "META-INF/MANIFEST.MF"));
    }

    private File getFile(final String requesURL) throws IOException {
        String url = requesURL;
        String entry = null;
        if (requesURL.contains("!")) {
            // request file content into an archive
            int entryIndex = requesURL.indexOf('!');
            entry = requesURL.substring(entryIndex + 1);
            url = requesURL.substring(0, entryIndex);
        }
        File cacheFile = cachedFile(requesURL);
        if (!cacheFile.exists()) {
            // High log level, but during regular operation this will indicate when an artifact is newly picked up, so useful to know.
            LOGGER.log(Level.INFO, "Downloading : " + url + " (not found in cache)");

            final File parentFile = cacheFile.getParentFile();
            if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
                throw new IllegalStateException("Failed to create non-existing directory " + parentFile);
            }
            try {
                OkHttpClient.Builder builder = new OkHttpClient.Builder();
                OkHttpClient client = builder.build();
                Request request = new Request.Builder().url(url).get().build();
                final Response response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    try (final ResponseBody body = HttpHelper.body(response)) {
                        try (InputStream inputStream = body.byteStream(); ByteArrayOutputStream baos = new ByteArrayOutputStream(); FileOutputStream fos = new FileOutputStream(cacheFile); TeeOutputStream tos = new TeeOutputStream(fos, baos)) {
                            @SuppressWarnings("resource")
                            InputStream is = inputStream;
                            if (entry != null) {
                                is = IOUtils.toInputStream("", StandardCharsets.UTF_8);
                                ZipInputStream zis = new ZipInputStream(inputStream);
                                ZipEntry ze = zis.getNextEntry();
                                while (ze != null) {
                                    if (entry.equals(ze.getName())) {
                                        is = zis;
                                        break;
                                    }
                                    ze = zis.getNextEntry();
                                }
                            }
                            IOUtils.copy(is, tos);
                            if (baos.size() <= CACHE_ENTRY_MAX_LENGTH) {
                                final String value = baos.toString("UTF-8");
                                LOGGER.log(Level.FINE, "Caching in memory: " + url + " with content: " + value);
                                this.cache.put(url, value);
                            }
                        }
                    }
                } else {
                    LOGGER.log(Level.INFO, "Received HTTP error response: " + response.code() + " for URL: " + url);
                    if (!cacheFile.mkdir()) {
                        LOGGER.log(Level.WARNING, "Failed to create cache 'not found' directory" + cacheFile);
                    }
                }
            } catch (RuntimeException e) {
                throw new IOException(e);
            }
        } else {
            if (cacheFile.isDirectory()) {
                // indicator that this is a cached error
                this.cache.put(requesURL, null);
                throw new IOException("Failed to retrieve content of " + requesURL + " (cached)");
            } else {
                // read from cached file
                if (cacheFile.length() <= CACHE_ENTRY_MAX_LENGTH) {
                    this.cache.put(requesURL, FileUtils.readFileToString(cacheFile, StandardCharsets.UTF_8));
                }
            }
        }
        return cacheFile;
    }

    private File cachedFile(final String requesURL) throws MalformedURLException {
        String urlBase64 = Base64.encodeBase64String(new URL(requesURL).getQuery().getBytes(StandardCharsets.UTF_8));
        return new File(cacheDirectory, urlBase64);
    }

    @Override
    public InputStream getZipFileEntry(MavenArtifact artifact, String path) throws IOException {
        File artifactFile = getFile(String.format(SEARCH_MAVEN_DOWNLOAD_URL, getUri(artifact.artifact)));
        if (artifactFile.exists()) {
            ZipInputStream zis = new ZipInputStream(new FileInputStream(artifactFile));
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                if (path.equals(ze.getName())) {
                    return zis;
                }
                ze = zis.getNextEntry();
            }
        }
        return null;
    }

    @Override
    public File resolve(ArtifactCoordinates artifact) throws IOException {
        /* Support loading files from local Maven repository to reduce redundancy */
        final String uri = getUri(artifact);
        final File localFile = new File(LOCAL_REPO, uri);
        if (localFile.exists()) {
            return localFile;
        }
        return getFile(String.format(SEARCH_MAVEN_DOWNLOAD_URL, uri));
    }

    private static final File LOCAL_REPO = new File(new File(System.getProperty("user.home")), ".m2/repository");

    @Override
    public String getRepositoryURL() {
    	return REPO_MAVEN_URL;
    }
}
