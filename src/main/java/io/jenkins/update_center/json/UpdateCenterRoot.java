package io.jenkins.update_center.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Functions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.util.VersionNumber;
import io.jenkins.update_center.BaseMavenRepository;
import io.jenkins.update_center.Deprecations;
import io.jenkins.update_center.JenkinsWar;
import io.jenkins.update_center.MavenRepository;
import io.jenkins.update_center.Plugin;
import io.jenkins.update_center.PluginUpdateCenterEntry;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class UpdateCenterRoot extends WithSignature {
    @JSONField
    @SuppressFBWarnings(value = "SS_SHOULD_BE_STATIC", justification = "Accessed by JSON serializer")
    public final String updateCenterVersion = "1";

    @JSONField
    public String connectionCheckUrl;

    @JSONField
    public String id;

    @JSONField
    public UpdateCenterCore core;

    @JSONField
    public Map<String, PluginUpdateCenterEntry> plugins = new TreeMap<>();

    @JSONField
    public List<UpdateCenterWarning> warnings;

    @JSONField
    public Map<String, UpdateCenterDeprecation> deprecations;

    public UpdateCenterRoot(MavenRepository repo, File warningsJsonFile) throws IOException {
        // load warnings
        final String warningsJsonText = String.join("", Files.readAllLines(warningsJsonFile.toPath(), StandardCharsets.UTF_8));
        warnings = Arrays.asList(JSON.parseObject(warningsJsonText, UpdateCenterWarning[].class));

        // load deprecations
        deprecations = new TreeMap<>(Deprecations.getDeprecatedPlugins().collect(Collectors.toMap(Functions.identity(), UpdateCenterRoot::deprecationForPlugin)));

        for (Plugin plugin : repo.listJenkinsPlugins()) {
            PluginUpdateCenterEntry entry = new PluginUpdateCenterEntry(plugin);
            plugins.put(plugin.getArtifactId(), entry);
        }

        TreeMap<VersionNumber, JenkinsWar> jenkinsWars = repo.getJenkinsWarsByVersionNumber();
        if (!jenkinsWars.isEmpty()) { // avoid break update site parser if no jenkins are found
            core = new UpdateCenterCore(jenkinsWars);
        }
    }

    private static UpdateCenterDeprecation deprecationForPlugin(String artifactId) {
        String deprecationUrl = Deprecations.getCustomDeprecationUri(artifactId);
        String noticeUrl = deprecationUrl != null ? deprecationUrl : BaseMavenRepository.getIgnoreNoticeUrl(artifactId);
        return new UpdateCenterDeprecation(noticeUrl);
    }
}
