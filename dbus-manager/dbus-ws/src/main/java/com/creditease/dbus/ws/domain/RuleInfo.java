package com.creditease.dbus.ws.domain;

import java.io.Serializable;

/**
 * Created by haowei6 on 2017/10/31.
 */
public class RuleInfo implements Serializable {

    private Long groupId;
    private String groupName;
    private String status;
    private Integer orderId;
    private String ruleTypeName;
    private String ruleGrammar;

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getRuleTypeName() {
        return ruleTypeName;
    }

    public void setRuleTypeName(String ruleTypeName) {
        this.ruleTypeName = ruleTypeName;
    }

    public String getRuleGrammar() {
        return ruleGrammar;
    }

    public void setRuleGrammar(String ruleGrammar) {
        this.ruleGrammar = ruleGrammar;
    }
}
