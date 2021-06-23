package com.aerospike.documentapi;

import com.aerospike.documentapi.pathparts.PathPart;

import java.util.ArrayList;
import java.util.List;

public class JsonPathObject {
    private final List<PathPart> pathParts;
    private boolean requiresJsonPathQuery;

    public JsonPathObject() {
        pathParts = new ArrayList<>();
    }

    public List<PathPart> getAccessPathParts() {
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
}
