package com.dataexa.insight.domain.security.spark.dataset.model;


import com.alibaba.fastjson.JSONObject;
import com.dataexa.insight.domain.security.spark.dataset.util.DateUtils;

import java.io.Serializable;

public class HotelRecord implements Serializable {
    private String zjhm;
    private String lgjd;
    private String lgwd;
    private String rzfh;
    private String tfsj;
    private String rzsj;
    private JSONObject diffFieldJsonObject;
    private JSONObject sameFieldJsonObject;
    private long leaveTimeMillSecond;
    private long comeTimeMillSecond;
    private long stayMillSecond;

    public HotelRecord(){

    }

    public JSONObject getDiffFieldJsonObject() {
        return diffFieldJsonObject;
    }

    public void setDiffFieldJsonObject(JSONObject diffFieldJsonObject) {
        this.diffFieldJsonObject = diffFieldJsonObject;
    }

    public JSONObject getSameFieldJsonObject() {
        return sameFieldJsonObject;
    }

    public void setSameFieldJsonObject(JSONObject sameFieldJsonObject) {
        this.sameFieldJsonObject = sameFieldJsonObject;
    }

    public HotelRecord(String zjhm, String rzfh, String tfsj, String rzsj, String lgbm, JSONObject diffFieldJsonObject,JSONObject sameFieldJsonObject) {
        this.zjhm = zjhm;
        this.rzfh = rzfh;
        this.tfsj = tfsj;
        this.rzsj = rzsj;
        this.lgbm = lgbm;
        this.diffFieldJsonObject =diffFieldJsonObject;
        this.sameFieldJsonObject=sameFieldJsonObject;
        setLeaveTimeMillSecond(DateUtils.parseYYYYMMDDHHMM2Date(tfsj).getTimeInMillis()/1000);
        setComeTimeMillSecond(DateUtils.parseYYYYMMDDHHMM2Date(rzsj).getTimeInMillis()/1000);
        stayMillSecond=leaveTimeMillSecond-comeTimeMillSecond;
    }

    public HotelRecord(String zjhm, String lgjd, String lgwd, String rzfh, String tfsj, String rzsj, JSONObject diffFieldJsonObject,
                       JSONObject sameFieldJsonObject, long leaveTimeMillSecond, long comeTimeMillSecond, long stayMillSecond, String lgbm) {
        this.zjhm = zjhm;
        this.lgjd = lgjd;
        this.lgwd = lgwd;
        this.rzfh = rzfh;
        this.tfsj = tfsj;
        this.rzsj = rzsj;
        this.diffFieldJsonObject = diffFieldJsonObject;
        this.sameFieldJsonObject = sameFieldJsonObject;
        this.leaveTimeMillSecond = leaveTimeMillSecond;
        this.comeTimeMillSecond = comeTimeMillSecond;
        this.stayMillSecond = stayMillSecond;
        this.lgbm = lgbm;
    }

    public long getStayMillSecond() {

        return stayMillSecond;
    }

    public void setStayMillSecond(long stayMillSecond) {
        this.stayMillSecond = stayMillSecond;
    }

    private String lgbm;

    public String getRzfh() {
        return rzfh;
    }

    public void setRzfh(String rzfh) {
        this.rzfh = rzfh;
    }


    public String getTfsj() {
        return tfsj;
    }

    public void setTfsj(String tfsj) {
        this.tfsj = tfsj;
    }

    public String getRzsj() {
        return rzsj;
    }

    public void setRzsj(String rzsj) {
        this.rzsj = rzsj;
    }

    public String getLgbm() {
        return lgbm;
    }

    public void setLgbm(String lgbm) {
        this.lgbm = lgbm;
    }


    public String getLgjd() {
        return lgjd;
    }

    public void setLgjd(String lgjd) {
        this.lgjd = lgjd;
    }

    public String getLgwd() {
        return lgwd;
    }

    public void setLgwd(String lgwd) {
        this.lgwd = lgwd;
    }

    public String getrzfh() {
        return rzfh;
    }

    public void setrzfh(String rzfh) {
        this.rzfh = rzfh;
    }


    public long getLeaveTimeMillSecond() {
        return leaveTimeMillSecond;
    }

    public void setLeaveTimeMillSecond(long leaveTimeMillSecond) {
        this.leaveTimeMillSecond = leaveTimeMillSecond;
    }

    public long getComeTimeMillSecond() {
        return comeTimeMillSecond;
    }

    public void setComeTimeMillSecond(long comeTimeMillSecond) {
        this.comeTimeMillSecond = comeTimeMillSecond;
    }

    public String getZjhm() {
        return zjhm;
    }

    public void setZjhm(String zjhm) {
        this.zjhm = zjhm;
    }
}
