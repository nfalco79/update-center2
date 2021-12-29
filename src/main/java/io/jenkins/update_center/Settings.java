package io.jenkins.update_center;

import io.jenkins.update_center.util.Environment;

public final class Settings {

    public static final String UPDATE_SITE_URL = Environment.getString("UPDATE_SITE_URL", "https://plugins.jenkins.io/");

    private Settings() {
    }
}
