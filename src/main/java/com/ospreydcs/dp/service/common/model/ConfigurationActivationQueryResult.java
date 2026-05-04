package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;

import java.util.List;

/**
 * Result of a paginated ConfigurationActivation query, bundling the current page of documents
 * with the next-page token (empty string when no further pages exist).
 */
public class ConfigurationActivationQueryResult {

    private final List<ConfigurationActivationDocument> documents;
    private final String nextPageToken;

    public ConfigurationActivationQueryResult(
            List<ConfigurationActivationDocument> documents,
            String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<ConfigurationActivationDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
