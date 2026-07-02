package fgm.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import fgm.common.statevector.FastAGMS;
import fgm.common.statevector.KeyFrequencyList;
import fgm.streamdata.StreamRecord;


import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FastAGMS.class, name = "FastAGMS"),
        @JsonSubTypes.Type(value = KeyFrequencyList.class, name = "KeyFrequencyList")
})
public interface Vector extends Serializable {
    void scale(Double scalar);

    void update(StreamRecord streamRecord, double frequency);

    void add(Vector vector);

    Vector subtract(Vector vector);

    Vector deepCopy();

    String getToken();

}
