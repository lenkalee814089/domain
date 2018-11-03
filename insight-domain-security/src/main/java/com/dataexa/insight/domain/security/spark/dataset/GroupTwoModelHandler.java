package com.dataexa.insight.domain.security.spark.dataset;

import com.dataexa.insight.domain.security.annotation.InsightComponent;
import com.dataexa.insight.spec.Operator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.annotation.meta.param;

/**
 * 同行模型组件
 */
public class GroupTwoModelHandler implements Operator {


    @InsightComponent( name = "网吧同行", description = "同行规则：同登录时间（间隔半个小时以内），同行次数加一")
    public static Dataset<Row>  barOnlineTwoModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "multiBarFilterNum", description = "多网吧同行次数阈值 >=",request = true) int multiBarFilterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "singleBarFilterNum", description = "单个网吧同行次数阈值 >=",request = true) int singleBarFilterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "SFZH", description = "身份证号",request = true) String SFZH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "WBDZ", description = "网吧地址",request = true) String WBDZ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "DLSJ", description = "登录时间",request = true) String DLSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "barCols", description = "网吧信息列，以分号分隔",request = true) String barCols) {

        return ynfkGroupTwoModel.barOnlineTwoModel(dataset,multiBarFilterNum,singleBarFilterNum,SFZH,WBDZ,DLSJ,barCols);
    }


    @InsightComponent( name = "客车同行", description = "同行规则：同客车班次（客车发车时间相同），同行次数加一")
    public static Dataset<Row>  busTravelTwoModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "multiBusFilterNum", description = "多客车同行次数阈值 >=",request = true) int multiBusFilterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "singleBusFilterNum", description = "单客车同行次数阈值 >=",request = true) int singleBusFilterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "KCBC", description = "客车编号",request = true) String KCBC,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "KCFCSJ", description = "客车发车时间",request = true) String KCFCSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "busCols", description = "客车信息列，以分号分隔",request = true) String busCols) {

        return ynfkGroupTwoModel.busTravelTwoModel(dataset,multiBusFilterNum,singleBusFilterNum,ZJHM,KCBC,KCFCSJ,busCols);
    }


    @InsightComponent( name = "客车邻座", description = "同行规则：同客车（客车发车时间相同）邻座大于2次的为同行人")
    public static Dataset<Row>  busTravelNeighbourModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "filterNum", description = "邻座次数阈值 >=",request = true) int filterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "KCBC", description = "客车编号",request = true) String KCBC,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "KCFCSJ", description = "客车发车时间",request = true) String KCFCSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZWH", description = "座位号",request = true) String ZWH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "busCols", description = "客车信息列，以分号分隔",request = true) String busCols) {

        return ynfkGroupTwoModel.busTravelNeighbourModel(dataset,filterNum,ZJHM,KCBC,KCFCSJ,ZWH,busCols);
    }


    @InsightComponent( name = "飞机同行", description = "同行规则：同飞机班次（航班号相同，离港时间相同），同行次数加一")
    public static Dataset<Row>  flightTravelTwoModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "filterNum", description = "同行次数阀值 >=",request = true) int filterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "HBH", description = "航班号",request = true) String HBH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "LG_RQ", description = "离港时间",request = true) String LG_RQ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "flightCols", description = "航班信息列，以分号分隔",request = true) String flightCols) {

        return ynfkGroupTwoModel.flightTravelTwoModel(dataset,filterNum,ZJHM,HBH,LG_RQ,flightCols);
    }


    @InsightComponent( name = "飞机邻座", description = "同行规则：同飞机班次（航班号相同，离港时间相同），邻座次数大于2的为同行人")
    public static Dataset<Row>  flightTravelNeighbourModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "filterNum", description = "同行次数阀值 >=",request = true) int filterNum,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "HBH", description = "航班号",request = true) String HBH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "LG_RQ", description = "离港时间",request = true) String LG_RQ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "LKZW", description = "离港时间",request = true) String LKZW,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "flightCols", description = "航班信息列，以分号分隔",request = true) String flightCols) {

        return ynfkGroupTwoModel.flightTravelNeighbourModel(dataset,filterNum,ZJHM,HBH,LG_RQ,LKZW,flightCols);
    }

    @InsightComponent( name = "铁路同行", description = "同行规则：同铁路班次（车次、出发时间、出发地、目的地相同），同铁路次数大于等于4的为铁路同行")
    public static Dataset<Row>  railwayTravelTwoModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "sameRailTimesThreshold", description = "同铁路次数阀值 >=",request = true) int sameRailTimesThreshold,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "GMSFHM", description = "公民身份证号码",request = true) String GMSFHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "SFD", description = "始发地",request = true) String SFD,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "MDD", description = "目的地",request = true) String MDD,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "CC", description = "车次",request = true) String CC,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "CXH", description = "车厢号",request = true) String CXH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZWH", description = "座位号",request = true) String ZWH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "FCSJ", description = "发车时间",request = true) String FCSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "railColums", description = "铁路同行人员内容相异的信息列,如座位号、车厢号.. 以分号分隔",request = true) String railColums,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "commonColums", description = "铁路同行人员内容相同的信息列,如车次、始发地、目的地.. 以分号分隔",request = true) String commonColums) {

        return ynfkGroupTwoModelJava.railWayTravelTwoModel (dataset,sameRailTimesThreshold,GMSFHM,SFD,MDD,CC,CXH,ZWH,FCSJ,railColums,commonColums);
    }
    @InsightComponent( name = "铁路邻座", description = "同行规则：铁路邻座位（车次、出发时间、出发地、目的地相、车厢号等相同.且座位相邻），同铁路邻座大于等于2次的为铁路邻座")
    public static Dataset<Row>  railwayTravelNeighbourModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "adjacentTimesThreshold", description = "邻座次数阀值 >=",request = true) int adjacentTimesThreshold,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "GMSFHM", description = "公民身份证号码",request = true) String GMSFHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "SFD", description = "始发地",request = true) String SFD,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "MDD", description = "目的地",request = true) String MDD,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "CC", description = "车次",request = true) String CC,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "CXH", description = "车厢号",request = true) String CXH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZWH", description = "座位号",request = true) String ZWH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "FCSJ", description = "发车时间",request = true) String FCSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "railColums", description = "铁路邻座人员内容相异的信息列,如座位号.. 以分号分隔",request = true)String railColums,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "commonColums", description = "铁路同行人员内容相同的信息列,如车次、始发地、目的地.. 以分号分隔",request = true) String commonColums) {
        return ynfkGroupTwoModelJava.railWayTravelNeighbourModel(dataset,adjacentTimesThreshold,GMSFHM,SFD,MDD,CC,CXH,ZWH,FCSJ,railColums,commonColums);
    }

    @InsightComponent( name = "旅馆同住宿", description = "同行规则：同旅馆（在同一旅馆入住时间重合度超过80%），同旅馆次数大于等于5的为旅馆同住宿")
    public static Dataset<Row>  hotelTravelTwoModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "sameHotelTimesThreshold", description = "同旅馆次数阈值 >=",request = true) int sameHotelTimesThreshold,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "TFSJ", description = "退房时间",request = true) String TFSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "RZSJ", description = "入住时间",request = true) String RZSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "LGBM", description = "旅馆编码",request = true) String LGBM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "RZFH", description = "入住房号",request = true) String RZFH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "hotelColums", description = "旅馆同行人员内容相异的信息列,如旅馆编码.. 以分号分隔",request = true) String hotelColums,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "commonColums", description = "旅馆同行人员内容相同的信息列,如旅馆编码、旅馆经纬度. 以分号分隔",request = true) String commonColums) {
        return ynfkGroupTwoModelJava.hotelTravelTwoModel(dataset,sameHotelTimesThreshold,ZJHM,RZSJ,LGBM,TFSJ,RZFH,hotelColums,commonColums);

    }
    @InsightComponent( name = "旅馆同房间", description = "同行规则：同房间（在两人同旅馆时间交际内入住同一房间），同房间次数大于等于2次的为旅馆同房间")
    public static Dataset<Row>  hotelTravelSameRoomModel(
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "dataset", description = "数据集",request = true) Dataset<Row> dataset,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "sameRoomTimesThreshold", description = "同房间次数阈值 >=",request = true) int sameRoomTimesThreshold,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "ZJHM", description = "证件号码",request = true) String ZJHM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "TFSJ", description = "退房时间",request = true) String TFSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "RZSJ", description = "入住时间",request = true) String RZSJ,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "LGBM", description = "旅馆编码",request = true) String LGBM,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "RZFH", description = "入住房号",request = true) String RZFH,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "hotelColums", description = "旅馆同宿人员内容相异的信息列,如旅馆编码.. 以分号分隔",request = true) String hotelColums,
            @com.dataexa.data.prepare.annotation.InsightComponentArg(name = "commonColums", description = "旅馆同宿人员内容相同的信息列,如入住时间、退房时间,以分号分隔",request = true) String commonColums) {
        return  ynfkGroupTwoModelJava.hotelTraveSameRoomModel(dataset,sameRoomTimesThreshold,ZJHM,TFSJ,RZSJ,LGBM,RZFH,hotelColums,commonColums);

    }




}
