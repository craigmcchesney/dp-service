package com.ospreydcs.dp.service.common.bson.configuration;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.TreeSet;

public class ConfigurationActivationDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String clientActivationId;
    private String configurationName;
    private String internalCategory;  // denormalized from Configuration.category at save time
    private Instant startTime;
    private Instant endTime;          // null = open-ended
    private String description;
    private String modifiedBy;

    public ConfigurationActivationDocument() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getClientActivationId() {
        return clientActivationId;
    }

    public void setClientActivationId(String clientActivationId) {
        this.clientActivationId = clientActivationId;
    }

    public String getConfigurationName() {
        return configurationName;
    }

    public void setConfigurationName(String configurationName) {
        this.configurationName = configurationName;
    }

    public String getInternalCategory() {
        return internalCategory;
    }

    public void setInternalCategory(String internalCategory) {
        this.internalCategory = internalCategory;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public static ConfigurationActivationDocument fromSaveConfigurationActivationRequest(
            SaveConfigurationActivationRequest request) {

        ConfigurationActivationDocument document = new ConfigurationActivationDocument();

        if (!request.getClientActivationId().isBlank()) {
            document.setClientActivationId(request.getClientActivationId());
        }

        document.setConfigurationName(request.getConfigurationName());

        // internalCategory is set later in the job/client after looking up the Configuration
        // startTime and endTime are required to be converted from proto Timestamp to Instant
        if (request.hasStartTime()) {
            document.setStartTime(TimestampUtility.instantFromTimestamp(request.getStartTime()));
        }

        if (request.hasEndTime()) {
            document.setEndTime(TimestampUtility.instantFromTimestamp(request.getEndTime()));
        }

        if (!request.getDescription().isBlank()) {
            document.setDescription(request.getDescription());
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

    public ConfigurationActivation toConfigurationActivation() {

        ConfigurationActivation.Builder builder = ConfigurationActivation.newBuilder();

        if (this.clientActivationId != null) {
            builder.setClientActivationId(this.clientActivationId);
        }

        if (this.configurationName != null) {
            builder.setConfigurationName(this.configurationName);
        }

        if (this.startTime != null) {
            builder.setStartTime(TimestampUtility.getTimestampFromInstant(this.startTime));
        }

        if (this.endTime != null) {
            builder.setEndTime(TimestampUtility.getTimestampFromInstant(this.endTime));
        }

        if (this.description != null) {
            builder.setDescription(this.description);
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
