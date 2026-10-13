package fgm.streamdata.implementation.stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import fgm.streamdata.StreamRecord;

public class WorldCupRecord implements StreamRecord {

    public WorldCupRecord() {
    }

    public WorldCupRecord(long timeStamp,
                          long clientID,
                          long objectID,
                          long size,
                          short method,
                          short status,
                          short type,
                          short server) {

        this.timeStamp = timeStamp;
        this.clientID = clientID;
        this.objectID = objectID;
        this.size = size;
        this.method = method;
        this.status = status;
        this.type = type;
        this.server = server;

    }

    @JsonProperty
    private long timeStamp;
    @JsonProperty
    private long clientID;
    @JsonProperty
    private long objectID;
    @JsonProperty
    private long size;
    @JsonProperty
    private short method;
    @JsonProperty
    private short status;
    @JsonProperty
    private short type;
    @JsonProperty
    private short server;

    @JsonIgnore
    public long getTimeStamp() {
            return this.timeStamp * 1000;
        }

    @JsonIgnore
    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getClientID() {
        return this.clientID;
    }

    public void setClientID(long clientID) {
        this.clientID = clientID;
    }

    public long getObjectID() {
        return this.objectID;
    }

    public void setObjectID(long objectID) {
        this.objectID = objectID;
    }

    public long getSize() {
        return this.size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getMethod() {
        return this.method;
    }

    public void setMethod(short method) {
        this.method = method;
    }

    public long getStatus() {
        return this.status;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public long getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public long getServer() {
        return this.server;
    }

    public void setServer(short server) {
        this.server = server;
    }

    @Override
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public Long getKey() {
        return this.clientID;
    }

}
