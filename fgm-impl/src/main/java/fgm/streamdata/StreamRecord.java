package fgm.streamdata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fgm.streamdata.implementation.stream.WorldCupRecord;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "record_type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = WorldCupRecord.class, name = "WorldCupRecord")
})
public interface StreamRecord extends Serializable {
    <T> T getKey(); // the stream record key
    long getTimeStamp();
}
