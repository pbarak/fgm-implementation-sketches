package fgm.common.messages;

import fgm.streamdata.StreamRecord;

import java.io.Serializable;

public class StatisticsMessage implements Serializable {

    private Integer round;
    private Integer subround;
    private Long streamRecordNum;
    private Long timeStamp; //wall clock time for logging and throughput statistics
    private String siteID;
    private String messageType;
    private StreamRecord streamRecord;

    private StatisticsMessage() {
    }

    private StatisticsMessage(Integer round, Integer subround, String siteID, String messageType) {
        this.round = round;
        this.subround = subround;
        this.timeStamp = System.currentTimeMillis();
        this.siteID = siteID;
        this.messageType = messageType;
    }

    private StatisticsMessage(Integer round, Integer subround, String siteID, String messageType, StreamRecord streamRecord) {
        this.round = round;
        this.subround = subround;
        this.timeStamp = System.currentTimeMillis();
        this.siteID = siteID;
        this.messageType = messageType;
        this.streamRecord = streamRecord;
    }

    public static StatisticsMessage getInstance(Integer round, Integer subround, String siteID, String messageType) {
        return new StatisticsMessage(round, subround, siteID, messageType);
    }

    public static StatisticsMessage getInstance() {
        return new StatisticsMessage();
    }

    public static StatisticsMessage getInstance(Integer round, Integer subround, String siteID, String messageType, StreamRecord streamRecord) {
        return new StatisticsMessage(round, subround, siteID, messageType, streamRecord);
    }

    private static final long serialVersionUID = 4L;

    public Integer getRound() {
        return round;
    }

    public void setRound(Integer round) {
        this.round = round;
    }

    public Integer getSubround() {
        return subround;
    }

    public void setSubround(Integer subround) {
        this.subround = subround;
    }

    public Long getStreamRecordNum() {
        return streamRecordNum;
    }

    public void setStreamRecordNum(Long streamRecordNum) {
        this.streamRecordNum = streamRecordNum;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getSiteID() {
        return siteID;
    }

    public void setSiteID(String siteID) {
        this.siteID = siteID;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public StreamRecord getRecord() {
        return this.streamRecord;
    }

    public void setRecord(StreamRecord streamRecord) {
        this.streamRecord = streamRecord;
    }


}
