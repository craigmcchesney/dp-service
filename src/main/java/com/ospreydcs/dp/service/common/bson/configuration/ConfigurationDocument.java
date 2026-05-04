package com.ospreydcs.dp.service.common.bson.configuration;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.common.Configuration;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.TreeSet;

public class ConfigurationDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String configurationName;
    private String category;
    private String description;
    private String parentConfigurationName;
    private String modifiedBy;

    public ConfigurationDocument() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getConfigurationName() {
        return configurationName;
    }

    public void setConfigurationName(String configurationName) {
        this.configurationName = configurationName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getParentConfigurationName() {
        return parentConfigurationName;
    }

    public void setParentConfigurationName(String parentConfigurationName) {
        this.parentConfigurationName = parentConfigurationName;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public static ConfigurationDocument fromSaveConfigurationRequest(SaveConfigurationRequest request) {

        ConfigurationDocument document = new ConfigurationDocument();

        document.setConfigurationName(request.getConfigurationName());
        document.setCategory(request.getCategory());

        if (!request.getDescription().isBlank()) {
            document.setDescription(request.getDescription());
        }

        if (!request.getParentConfigurationName().isBlank()) {
            document.setParentConfigurationName(request.getParentConfigurationName());
        }

        if (!request.getModifiedBy().isBlank()) {
            document.setModifiedBy(request.getModifiedBy());
        }

        // normalize tags: lowercase, unique, sorted
        if (!request.getTagsList().isEmpty()) {
            final TreeSet<String> normalizedTags = new TreeSet<>();
            for (String tag : request.getTagsList()) {
                normalizedTags.add(tag.toLowerCase());
            }
            document.setTags(new ArrayList<>(normalizedTags));
        }

        if (!request.getAttributesList().isEmpty()) {
            document.setAttributes(AttributesUtility.attributeMapFromList(request.getAttributesList()));
        }

        return document;
    }

    public Configuration toConfiguration() {

        Configuration.Builder builder = Configuration.newBuilder();

        if (this.configurationName != null) {
            builder.setConfigurationName(this.configurationName);
        }

        if (this.category != null) {
            builder.setCategory(this.category);
        }

        if (this.description != null) {
            builder.setDescription(this.description);
        }

        if (this.parentConfigurationName != null) {
            builder.setParentConfigurationName(this.parentConfigurationName);
        }

        if (this.modifiedBy != null) {
            builder.setModifiedBy(this.modifiedBy);
        }

        if (this.getTags() != null) {
            builder.addAllTags(this.getTags());
        }

        if (this.getAttributes() != null) {
            builder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        if (this.getCreatedAt() != null) {
            builder.setCreatedTime(TimestampUtility.getTimestampFromInstant(this.getCreatedAt()));
        }

        if (this.getUpdatedAt() != null) {
            builder.setUpdatedTime(TimestampUtility.getTimestampFromInstant(this.getUpdatedAt()));
        }

        return builder.build();
    }
}
