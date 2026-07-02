package fgm.common.messages;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class PhiMessage implements Serializable {

    private static final long serialVersionUID = 2L;
    @JsonProperty
    private String siteId;
    @JsonProperty
    private Integer currentRound;
    @JsonProperty
    private Integer currentSubround;
    @JsonProperty
    private Double phi;

    public PhiMessage() {
        this.siteId = null;
        this.currentRound = 0;
        this.currentSubround = 0;
        this.phi = 0.0d;
    }

    public PhiMessage(String siteId, Integer currentRound, Integer currentSubround) {
        this.siteId = siteId;
        this.currentRound = currentRound;
        this.currentSubround = currentSubround;
    }

    public static PhiMessage getInstance() {
        return new PhiMessage();
    }

    public String getSiteId() {
        return this.siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public Integer getCurrentRound() {
        return this.currentRound;
    }

    public void setCurrentRound(int currentRound) {
        this.currentRound = currentRound;
    }

    public Integer getCurrentSubround() {
        return this.currentSubround;
    }

    public void setCurrentSubround(Integer currentSubround) {
        this.currentSubround = currentSubround;
    }

    public void setPhi(Double phi) {
        this.phi = phi;
    }

    @JsonIgnore
    public Double getPhiValue() {
        return this.phi;
    }


}
