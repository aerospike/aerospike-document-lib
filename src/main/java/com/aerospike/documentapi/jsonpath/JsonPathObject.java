package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.jsonpath.pathpart.PathPart;

import java.util.ArrayList;
import java.util.List;

public class JsonPathObject {

    private final List<PathPart> pathParts;
    private String jsonPathSecondStepQuery;
    private boolean requiresJsonPathQuery;

    public JsonPathObject() {
        pathParts = new ArrayList<>();
    }

    public JsonPathObject copy() {
        String newJsonPathSecondStepQuery = getJsonPathSecondStepQuery();
        boolean newRequiresJsonPathQuery = requiresJsonPathQuery();

        JsonPathObject newJsonPathObject = new JsonPathObject();
        for (PathPart pathPart : pathParts) {
            newJsonPathObject.addPathPart(pathPart);
        }
        newJsonPathObject.setJsonPathSecondStepQuery(newJsonPathSecondStepQuery);
        newJsonPathObject.setRequiresJsonPathQuery(newRequiresJsonPathQuery);
        return newJsonPathObject;
    }

    public List<PathPart> getPathParts() {
        return pathParts;
    }

    public void addPathPart(PathPart pathPart) {
        pathParts.add(pathPart);
    }

    public boolean requiresJsonPathQuery() {
        return requiresJsonPathQuery;
    }

    public void setRequiresJsonPathQuery(boolean status) {
        requiresJsonPathQuery = status;
    }

    public String getJsonPathSecondStepQuery() {
        return jsonPathSecondStepQuery;
    }

    public void setJsonPathSecondStepQuery(String jsonPathSecondStepQuery) {
        this.jsonPathSecondStepQuery = jsonPathSecondStepQuery;
    }
}
