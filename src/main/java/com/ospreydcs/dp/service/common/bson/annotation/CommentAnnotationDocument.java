package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@BsonDiscriminator(key = "type", value = "COMMENT")
public class CommentAnnotationDocument extends AnnotationDocument {

    private String comment;

    public static CommentAnnotationDocument fromCreateRequest(CreateAnnotationRequest request) {
        CommentAnnotationDocument document = new CommentAnnotationDocument();
        document.applyRequestFieldValues(request);
        document.setComment(request.getCommentDetails().getComment());
        return document;
    }

    public List<String> diffRequestDetails(CreateAnnotationRequest request) {

        final List<String> diffs = new ArrayList<>();

        String requestComment = "";
        if (request.hasCommentDetails()) {
            requestComment = request.getCommentDetails().getComment();
        }
        if (! Objects.equals(requestComment, this.getComment())) {
            final String msg = "comment mismatch: " + this.getComment() + " expected: " + requestComment;
            diffs.add(msg);
        }

        return diffs;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
