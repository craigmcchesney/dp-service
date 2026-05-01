package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;

import java.util.List;

/**
 * Result of a paginated Configuration query, bundling the current page of documents
 * with the next-page token (empty string when no further pages exist).
 */
public class ConfigurationQueryResult {

    private final List<ConfigurationDocument> documents;
    private final String nextPageToken;

    public ConfigurationQueryResult(List<ConfigurationDocument> documents, String nextPageToken) {
        this.documents = documents;
        this.nextPageToken = nextPageToken;
    }

    public List<ConfigurationDocument> getDocuments() {
        return documents;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
