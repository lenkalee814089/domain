package com.dataexa.insight.domain.security.spark.dataset.model;


import com.alibaba.fastjson.JSONObject;
import com.dataexa.insight.domain.security.spark.dataset.util.DateUtils;
import org.apache.log4j.Logger;

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
        this.rzsj = rzsj;
        this.setTfsj(tfsj);
        this.lgbm = lgbm;
        this.diffFieldJsonObject =diffFieldJsonObject;
        this.sameFieldJsonObject=sameFieldJsonObject;
        this.setLeaveTimeMillSecond(DateUtils.parseYYYYMMDDHHMM2Date(this.tfsj).getTimeInMillis()/1000);
        this.setComeTimeMillSecond(DateUtils.parseYYYYMMDDHHMM2Date(this.rzsj).getTimeInMillis()/1000);
        this.stayMillSecond=leaveTimeMillSecond-comeTimeMillSecond;
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
        if (tfsj.endsWith("error")||tfsj.isEmpty()){
//            System.out.println("tfsj检测到error或空字符串:"+tfsj);
            this.tfsj=DateUtils.plusOneDay(this.rzsj);
//            System.out.println("重新设定为"+rzsj+"的后一天:"+this.tfsj);
        }

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
