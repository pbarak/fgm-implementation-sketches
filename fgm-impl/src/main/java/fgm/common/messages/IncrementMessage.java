package fgm.common.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class IncrementMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    @JsonProperty
    private String siteId;
    @JsonProperty
    private Integer currentRound;
    @JsonProperty
    private Integer currentSubround;
    @JsonProperty
    private Integer increment;


    public IncrementMessage() {
        this.siteId = null;
        this.currentRound = 0;
        this.currentSubround = 0;
    }

    public IncrementMessage(String siteId, Integer currentRound, Integer currentSubround, Integer increment) {
        this.siteId = siteId;
        this.currentRound = currentRound;
        this.currentSubround = currentSubround;
        this.increment = increment;
    }

    public static IncrementMessage getInstance() {
        return new IncrementMessage();
    }

    public Integer getCurrentRound() {
        return this.currentRound;
    }

    public void setCurrentRound(Integer currentRound) {
        this.currentRound = currentRound;
    }

    public Integer getCurrentSubround() {
        return this.currentSubround;
    }

    public void setCurrentSubround(Integer currentSubround) {
        this.currentSubround = currentSubround;
    }

    public void setIncrementCounter(Integer increment) {
        this.increment = increment;
    }

    public String getSiteId() {
        return this.siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    @JsonIgnore
    public Integer getIncrementCounter() {
        return this.increment;
    }
}
