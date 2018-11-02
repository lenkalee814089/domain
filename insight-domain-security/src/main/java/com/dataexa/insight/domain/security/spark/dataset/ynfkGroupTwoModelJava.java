package com.dataexa.insight.domain.security.spark.dataset;

import com.alibaba.fastjson.JSONObject;
import com.dataexa.insight.domain.security.spark.dataset.model.HotelRecord;
import com.dataexa.insight.domain.security.spark.dataset.model.RailwayRecord;
import com.dataexa.insight.domain.security.spark.dataset.util.HotelFindUtils;
import com.dataexa.insight.domain.security.spark.dataset.util.RailwayFindUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.annotation.meta.param;

import java.util.*;

/**
 * auther:lijy
 *
 * 同行模型
 *
 */
public class ynfkGroupTwoModelJava {


    /**
     * 旅馆同行
     * 同行规则：同旅馆（入住退房时间重合度超过80%），同行次数加一
     * 旅馆同行次数不小于5的为同行人
     * 同房间不小于1的为同宿(同房间)人
     * @param df 数据集
     * @param zjhm 证件号码
     * @param tfsj 退房时间
     * @param rzsj 入住时间
     * @param lgbm 旅馆编码
     * @param hotelDiffColums 两同行人记录中,值不同的字段, 如入住时间 退房时间.. 字段以分号";"分隔开.
     * @param hotelCommonColums 两同行人记录中,值相同的字段 如旅馆编码 旅馆经纬度.. 字段以分号";"分隔开.
     * @return
     */
    private static JavaRDD<Row> hotelTwoCompute(Dataset<Row> df,
                                               String zjhm, String tfsj, String rzfh, String rzsj, String lgbm , String hotelDiffColums,String hotelCommonColums) {

        JavaRDD<Row> rdd = df.javaRDD();
        JavaRDD<Row> rowJavaRDD = rdd.filter(row -> row.getAs(zjhm) != null && row.getAs(tfsj) != null && row.getAs(rzsj) != null && row.getAs(lgbm) != null)
                .map(row -> {

                    JSONObject diffJsonObject = new JSONObject();
                    String[] hotelDiffColumnsSPlit = hotelDiffColums.split(";");
                    for (String colum : hotelDiffColumnsSPlit) {
                        diffJsonObject.put(colum, row.getAs(colum));
                    }

                    JSONObject commonJsonObject  = new JSONObject();
                    String[] hotelCommonColumnsSPlit = hotelCommonColums.split(";");
                    for (String colum : hotelCommonColumnsSPlit) {
                        commonJsonObject.put(colum, row.getAs(colum));
                    }

                    HotelRecord record = new HotelRecord(
                            row.getAs(zjhm),
                            row.getAs(rzfh),
                            row.getAs(tfsj),
                            row.getAs(rzsj),
                            row.getAs(lgbm),
                            diffJsonObject,
                            commonJsonObject
                    );
                    System.out.println(record.toString());
                    return record;


                    //根据不同的证件号划分ｒｅｃｏｒｄ
                }).mapToPair(record -> new Tuple2<String, HotelRecord>(record.getLgbm(), record))

                //让同旅馆的记录都到同一个区
                .partitionBy(new HashPartitioner(rdd.getNumPartitions()))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<HotelRecord>> map = new HashMap<>();
                    List<HotelRecord> recordList;//某个旅馆的所有record

                    while (it.hasNext()) {
                        HotelRecord record = it.next()._2;
                        recordList = map.getOrDefault(record.getLgbm(), new LinkedList<HotelRecord>());
                        recordList.add(record);
                        map.put(record.getLgbm(), recordList);
                    }

                    //同个旅馆的所有记录
                    List<HotelRecord> recordsOfHotel;

                    //tuple(a:b, 存放a,b同旅馆和同宿信息的list数组)
                    LinkedList<Tuple2<String, List<JSONObject>[]>> listOfHotelRelations = new LinkedList<>();

                    Map<String, List<JSONObject>[]> relationsInHotelMap;

                    //遍历map的每个list,一个list代表一个旅馆，放入处理函数
                    for (Map.Entry<String, List<HotelRecord>> listEntry : map.entrySet()) {

                        recordsOfHotel = listEntry.getValue();

                        //得到每个旅馆所有住宿人员两两之间关系eg:  a:b-> arr{3,1} ps:3为同行次数,1为同住宿次数
                        relationsInHotelMap = HotelFindUtils.getResultMap(recordsOfHotel);

                        relationsInHotelMap.entrySet().forEach(x -> listOfHotelRelations.add(new Tuple2<String, List<JSONObject>[]>(x.getKey(), x.getValue())));

                    }
                    return listOfHotelRelations.iterator();
                }).groupBy(tuple -> tuple._1).map(x -> { //x:  key -> (),(),(),()....
                    String key = x._1;
                    String personA = key.split(":")[0];
                    String personB = key.split(":")[1];
                    //将所有a:b的同旅馆,同宿记录合并
                    List<JSONObject>[] sumArr = new List[]{new LinkedList<>(), new LinkedList<>(),new LinkedList<>(), new LinkedList<>()};
                    //相同两人关系的同行次数,同宿次数累加
                    x._2.forEach(tuple2 -> {
                        sumArr[0].addAll(tuple2._2[0]);
                        sumArr[1].addAll(tuple2._2[1]);
                        sumArr[2].addAll(tuple2._2[2]);
                        sumArr[3].addAll(tuple2._2[3]);
                    });

                    //用","连接
                    String sameHotelInfo = "";
                    String sameRoomInfo = "";
                    String sameHotelCommonInfo ="";
                    String sameRoomlCommonInfo ="";
                    for (JSONObject jsonObject : sumArr[0]) {
                        sameHotelInfo += jsonObject.toJSONString() + ",";
                    }
                    for (JSONObject jsonObject : sumArr[1]) {
                        sameRoomInfo += jsonObject.toJSONString() + ",";
                    }
                    for (JSONObject jsonObject : sumArr[2]) {
                        sameHotelCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[3]) {
                        sameRoomlCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    //去除最后一个逗号
                    if (sameHotelInfo.length()!=0){
                        sameHotelInfo = sameHotelInfo.substring(0, sameHotelInfo.length() - 1);
                    }
                    if (sameRoomInfo.length()!=0){
                        sameRoomInfo = sameRoomInfo.substring(0, sameRoomInfo.length() - 1);
                    }
                    if (sameHotelCommonInfo.length()!=0){
                        sameHotelCommonInfo = sameHotelCommonInfo.substring(0, sameHotelCommonInfo.length() - 1);
                    }
                    if (sameRoomlCommonInfo.length()!=0){
                        sameRoomlCommonInfo = sameRoomlCommonInfo.substring(0, sameRoomlCommonInfo.length() - 1);
                    }


                    //人员A,人员B,同行次数,同宿次数,同行信息,同宿信息
                    return RowFactory.create(personA, personB, sumArr[0].size() / 2 + "", sumArr[1].size() / 2 + "", sameHotelInfo, sameRoomInfo,sameHotelCommonInfo,sameRoomlCommonInfo);
                });




        return rowJavaRDD ;

    }




    /**
     * 铁路同行
     * 同行规则：同铁路班次（发车时间相同,出发点,目的地,车次相同），同行次数加一
     * 同铁路次数不小于4的为同行人
     * 邻座次数不小于2次的为同行人
     * @param df 数据集
     * @param GMSFHM 公民身份证号码
     * @param SFD 始发地
     * @param MDD 目的地
     * @param CC  车次
     * @param CXH  车厢号
     * @param ZWH 座位号
     * @param FCSJ 发车时间
     * @param railColums 火车信息列 以分号";"分隔
     * @returdn
     */
    private static JavaRDD<Row> railWayCompute(Dataset<Row> df, String GMSFHM, String SFD ,
                                              String MDD, String CC, String CXH , String ZWH, String FCSJ , String railColums,String railwayCommonColums) {

        JavaRDD<Row> rdd = df.javaRDD();
        JavaRDD<Row> rowJavaRDD = rdd.filter(row -> row.getAs(GMSFHM) != null && row.getAs(SFD) != null && row.getAs(MDD) != null
                && row.getAs(CC) != null && row.getAs(CXH) != null && row.getAs(ZWH) != null && row.getAs(FCSJ) != null)
                .map(row -> {

                    JSONObject diffJsonObject = new JSONObject();
                    String[] railColumnsSPlit = railColums.split(";");
                    for (String colum : railColumnsSPlit) {
                        diffJsonObject.put(colum, row.getAs(colum));
                    }
                    JSONObject commonJsonObject  = new JSONObject();
                    String[] commonColumnsSPlit = railwayCommonColums.split(";");
                    for (String colum : commonColumnsSPlit) {
                        commonJsonObject.put(colum, row.getAs(colum));
                    }

                    RailwayRecord record = new RailwayRecord(
                            row.getAs(GMSFHM),
                            row.getAs(SFD),
                            row.getAs(MDD),
                            row.getAs(CC),
                            row.getAs(CXH),
                            row.getAs(ZWH),
                            row.getAs(FCSJ),
                            diffJsonObject,
                            commonJsonObject
                    );

                    System.out.println(record.toString());

                    return record;

                    //根据不同的证件号划分record
                }).mapToPair(record -> new Tuple2<>(record.getCC(), record))

                //让同车次的记录都到同一个区
                .partitionBy(new HashPartitioner(rdd.getNumPartitions()))

                //同个分区的record 进行分析处运算
                .mapPartitions(it -> {
                    Map<String, List<RailwayRecord>> map = new HashMap<>();
                    List<RailwayRecord> recordList;
                    Iterator iterator = null;

                    while (it.hasNext()) {
                        RailwayRecord record = it.next()._2;
                        recordList = map.getOrDefault(record.getCC(), new LinkedList<RailwayRecord>());
                        recordList.add(record);
                        map.put(record.getCC(), recordList);
                    }

                    List<RailwayRecord> recordsOfCC;

                    //tuple(a:b, 存放a,b同行和邻座信息的list数组)
                    List<Tuple2<String, List<JSONObject>[]>> listOfCCRelations = new LinkedList<>();

                    Map<String, List<JSONObject>[]> relationsInCC;
                    //遍历map的每个list，放入处理函数
                    for (Map.Entry<String, List<RailwayRecord>> listEntry : map.entrySet()) {

                        recordsOfCC = listEntry.getValue();

                        relationsInCC = RailwayFindUtils.getResultMap(recordsOfCC);

                        relationsInCC.entrySet().forEach(x -> listOfCCRelations.add(new Tuple2<String, List<JSONObject>[]>(x.getKey(), x.getValue())));

                    }
                    return listOfCCRelations.iterator();
                }).groupBy(tuple2 -> tuple2._1).map(x -> {//x:  key -> {list,list}....
                    String key = x._1;
                    String personA = key.split(":")[0];
                    String personB = key.split(":")[1];
                    //将所有a:b的同行记录合并
                    List<JSONObject>[] sumArr = new List[]{new LinkedList<>(), new LinkedList<>(),new LinkedList<>(), new LinkedList<>()};
                    //相同两人关系的同铁路次数,邻座次数累加
                    x._2.forEach(tuple2 -> {
                        sumArr[0].addAll(tuple2._2[0]);
                        sumArr[1].addAll(tuple2._2[1]);
                        sumArr[2].addAll(tuple2._2[2]);
                        sumArr[3].addAll(tuple2._2[3]);
                    });
                    //用","连接
                    String sameRailInfo = "";
                    String adjacentInfo = "";
                    String sameRailCommonInfo ="";
                    String adjacentCommonInfo ="";
                    for (JSONObject jsonObject : sumArr[0]) {
                        sameRailInfo += jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[1]) {
                        adjacentInfo += jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[2]) {
                        sameRailCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    for (JSONObject jsonObject : sumArr[3]) {
                        adjacentCommonInfo+=jsonObject.toJSONString()+",";
                    }
                    //去除最后一个逗号
                    if (sameRailInfo.length()!=0){
                        sameRailInfo=sameRailInfo.substring(0,sameRailInfo.length()-1);
                    }
                    if (adjacentInfo.length()!=0){
                        adjacentInfo=adjacentInfo.substring(0,adjacentInfo.length()-1);
                    }
                    if (sameRailCommonInfo.length()!=0){
                        sameRailCommonInfo = sameRailCommonInfo.substring(0, sameRailCommonInfo.length() - 1);
                    }
                    if (adjacentCommonInfo.length()!=0){
                        adjacentCommonInfo = adjacentCommonInfo.substring(0, adjacentCommonInfo.length() - 1);
                    }


                    //字段说明: 人员A,人员B,同铁路次数,邻座次数,同铁路信息,邻座的信息
                    return RowFactory.create(personA, personB, sumArr[0].size()/2 + "", sumArr[1].size()/2 + "", sameRailInfo, adjacentInfo,sameRailCommonInfo,adjacentCommonInfo);
                });

        return rowJavaRDD ;
    }


    /**
     * 旅馆同行
     * @param dataset
     * @param sameHotelTimesThreshold 同旅馆次数阈值
     * @param zjhm
     * @param rzsj
     * @param lgbm
     * @param tfsj
     * @param rzfh
     * @param hotelColums
     * @return
     */
    public static Dataset<Row> hotelTravelTwoModel (Dataset<Row> dataset,int sameHotelTimesThreshold,String zjhm,String rzsj,String lgbm,String tfsj,String rzfh,String hotelColums,String commonColums){

        //结果转换为df

       // String fildNames = "zjhm1,zjhm2,sameHotelTimes,sameRoomTimes,sameHotelInfo,sameRoomInfo,sameHotelCommonInfo,sameRoomlCommonInfo";
        String fildNames = "zjhm1,zjhm2,sameHotelTimes,sameHotelInfo,commonColums";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : fildNames.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = hotelTwoCompute(dataset, zjhm, tfsj, rzfh, rzsj, lgbm, hotelColums,commonColums);
        JavaRDD<Row> javaRDD = rowJavaRDD.map(row -> RowFactory.create(row.get(0), row.get(1), row.get(2), row.get(4), row.get(6)))
                .filter(row -> Integer.parseInt(row.getString(2)) >= sameHotelTimesThreshold);

        Dataset<Row> resultDataFrame = dataset.sparkSession().createDataFrame(javaRDD, schema);

        return resultDataFrame ;
    }

    /**
     * 旅馆同住
     * @param dataset
     * @param sameRoomTimesThreshold
     * @param zjhm
     * @param rzsj
     * @param lgbm
     * @param tfsj
     * @param rzfh
     * @param hotelColums
     * @return
     */
    public static Dataset<Row> hotelTraveSameRoomModel (Dataset<Row> dataset,int sameRoomTimesThreshold,String zjhm,String rzsj,String lgbm,String tfsj,String rzfh,String hotelColums,String commonColums){
        // String fildNames = "zjhm1,zjhm2,sameHotelTimes,sameRoomTimes,sameHotelInfo,sameRoomInfo,sameHotelCommonInfo,sameRoomlCommonInfo";
        String fildNames = "zjhm1,zjhm2,sameRoomTimes,sameRoomInfo,commonInfo";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : fildNames.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = hotelTwoCompute(dataset, zjhm, tfsj, rzfh, rzsj, lgbm, hotelColums,commonColums);
        JavaRDD<Row> javaRDD = rowJavaRDD.map(row -> RowFactory.create(row.get(0), row.get(1), row.get(3), row.get(5), row.get(7)))
                .filter(row -> Integer.parseInt(row.getString(2)) >= sameRoomTimesThreshold);

        Dataset<Row> resultDataFrame = dataset.sparkSession().createDataFrame(javaRDD, schema);

        return resultDataFrame ;
    }

    /**
     * 铁路同行
     * @param dataset
     * @param sameRailTimesThreshold
     * @param GMSFHM
     * @param SFD
     * @param MDD
     * @param CC
     * @param CXH
     * @param ZWH
     * @param FCSJ
     * @param railColums
     * @return
     */
    public static Dataset<Row> railWayTravelTwoModel (Dataset<Row> dataset,int sameRailTimesThreshold,String GMSFHM,String SFD,String MDD,
                                                      String CC,String CXH,String ZWH,String FCSJ,String railColums,String commonColums){
        //生成scheme
        String fildNames = "GMSFZH1,GMSFZH2,sameRailTimes,sameRailInfo,sameRailCommonInfo";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : fildNames.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = railWayCompute(dataset, GMSFHM, SFD, MDD, CC, CXH, ZWH, FCSJ, railColums,commonColums);

        // "GMSFZH1,GMSFZH2,sameRailTimes,adjacentTimes,sameRailInfo,adjacentInfo,sameRailCommonInfo,adjacentCommonInfo"
        JavaRDD<Row> javaRDD = rowJavaRDD.map(row -> RowFactory.create(row.get(0), row.get(1), row.get(2), row.get(4), row.get(6)))
                .filter(row -> Integer.parseInt(row.getString(2)) >= sameRailTimesThreshold);

        Dataset<Row> resultDataFrame = dataset.sparkSession().createDataFrame(javaRDD, schema);

        return resultDataFrame ;
    }

    /**
     * 铁路邻座
     * @param dataset
     * @param adjacentTimesThreshold
     * @param GMSFHM
     * @param SFD
     * @param MDD
     * @param CC
     * @param CXH
     * @param ZWH
     * @param FCSJ
     * @param railColums
     * @return
     */
    public static Dataset<Row> railWayTravelNeighbourModel (Dataset<Row> dataset,int adjacentTimesThreshold,String GMSFHM,String SFD,String MDD,
                                                            String CC,String CXH,String ZWH,String FCSJ,String railColums,String commonColums){
        //生成scheme
        String fildNames = "GMSFZH1,GMSFZH2,adjacentTimes,adjacentInfo,adjacentCommonInfo";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : fildNames.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = railWayCompute(dataset, GMSFHM, SFD, MDD, CC, CXH, ZWH, FCSJ, railColums,commonColums);

        // "GMSFZH1,GMSFZH2,sameRailTimes,adjacentTimes,sameRailInfo,adjacentInfo,sameRailCommonInfo,adjacentCommonInfo"
        JavaRDD<Row> javaRDD = rowJavaRDD.map(row -> RowFactory.create(row.get(0), row.get(1), row.get(3), row.get(5), row.get(7)))
                .filter(row -> row.getString(3)!=""&&Integer.parseInt(row.getString(2)) >= adjacentTimesThreshold);

        Dataset<Row> resultDataFrame = dataset.sparkSession().createDataFrame(javaRDD, schema);

        return resultDataFrame ;
    }

}
