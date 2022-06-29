package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

/**
 * @author : bufan
 * @date : 2022/6/29
 */
@JsonPropertyOrder({
        "code",
        "message",
        "totalTime",
})
@JsonIgnoreProperties({})
public class Message {
    private Integer code = 200;
    private String message;
    private Long totalTime;

    private String jobInfo;
    private String exceptionMsg;
    private Long createSourceTableTime;
    private Long createCloneTableTime;
    private Long exportFromSourceTime;
    private Long loadInCloneTime;

    public Integer getCode() {
        return code;
    }

    public Message setCode(Integer code) {
        this.code = code;
        return this;
    }

    public String getMessage() {
        if(StringUtil.isNullOrEmpty(this.message)){
            StringBuilder sb = new StringBuilder();
            if(StringUtil.nonEmpty(this.jobInfo))
                sb.append("Job Info : ").append(this.jobInfo).append("\n");
            if(StringUtil.nonEmpty(this.exceptionMsg)){
                sb.append("Exception Message : ").append(this.exceptionMsg).append("\n");
            }else {
                if(Objects.nonNull(this.createSourceTableTime))
                    sb.append("Create Source Table Spend Time : ").append(this.createSourceTableTime).append("\n");
                if(Objects.nonNull(this.createCloneTableTime))
                    sb.append("Create Clone Table Spend Time : ").append(this.createCloneTableTime).append("\n");
                if(Objects.nonNull(this.exportFromSourceTime))
                    sb.append("Export Data From Source Spend Time : ").append(this.exportFromSourceTime).append("\n");
                if(Objects.nonNull(this.loadInCloneTime))
                    sb.append("load Data In Clone Spend Time : ").append(this.loadInCloneTime).append("\n");
            }
            this.message = sb.toString();
        }
        return message;
    }

    public Message setMessage(String message) {
        this.message = message;
        return this;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public Message setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
        return this;
    }

    public String getJobInfo() {
        return jobInfo;
    }

    public Message setJobInfo(String jobInfo) {
        this.jobInfo = jobInfo;
        return this;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    public Message setExceptionMsg(String exceptionMsg) {
        this.exceptionMsg = exceptionMsg;
        return this;
    }

    public Long getCreateSourceTableTime() {
        return createSourceTableTime;
    }

    public Message setCreateSourceTableTime(Long createSourceTableTime) {
        this.createSourceTableTime = createSourceTableTime;
        return this;
    }

    public Long getCreateCloneTableTime() {
        return createCloneTableTime;
    }

    public Message setCreateCloneTableTime(Long createCloneTableTime) {
        this.createCloneTableTime = createCloneTableTime;
        return this;
    }

    public Long getExportFromSourceTime() {
        return exportFromSourceTime;
    }

    public Message setExportFromSourceTime(Long exportFromSourceTime) {
        this.exportFromSourceTime = exportFromSourceTime;
        return this;
    }

    public Long getLoadInCloneTime() {
        return loadInCloneTime;
    }

    public Message setLoadInCloneTime(Long loadInCloneTime) {
        this.loadInCloneTime = loadInCloneTime;
        return this;
    }
}
