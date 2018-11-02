package com.dataexa.insight.domain.security.spark.dataset

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * auther:wangfc
  *
  * 同行模型
  *
  */
object ynfkGroupTwoModel {

  /**
    * 网吧同行
    * 同行规则：同登录时间（间隔半个小时以内），同行次数加一
    * 单个网吧地址同行次数为5的，为同行人
    * 多个网吧地址同行次数为3的，为同行人
    * @param df
    * @param multiBarFilterNum 多网吧同行次数阈值 >=
    * @param singleBarFilterNum 单个网吧同行次数阈值 >=
    * @param SFZH 身份证号
    * @param WBDZ 网吧地址
    * @param DLSJ 登录时间
    * @param barCols 网吧信息列 以分号分隔
    * @return
    */
  def barOnlineTwoModel(df:Dataset[Row],multiBarFilterNum:Int,singleBarFilterNum:Int,SFZH:String,WBDZ:String,DLSJ:String,barCols:String)={
    // A乘客_B乘客==同行次数==航班号_离港时间,航班号_离港时间,... //
    import org.apache.spark.sql.functions._
    val columns = barCols.split(";")
    val session = df.sparkSession
    val rdd = df.filter(col(SFZH).isNotNull && col(WBDZ).isNotNull && col(DLSJ).isNotNull)
      .rdd
    // 转化成（k,v）再 按照 网吧地址，row 分组
    val kvRdd = rdd.map(x=>(x.getAs(WBDZ).toString,x))
    val groupedRDD: RDD[(String, Iterable[Row])] = kvRdd.groupByKey()
    //    groupedRDD.foreach(x=>{println(x._1)})
    // （A乘客-B乘客，row）
    val relatedPersonRdd: RDD[(String, String)] = groupedRDD.flatMap(x => {
      // 对组内所有人按照身份证号 从小到大 排序
      val groupPerson: List[Row] = x._2.toList.sortBy(_.getAs[String](SFZH))
      // 目标结果集合：（A乘客-B乘客，rowJson）
      val tuplesList = new ListBuffer[Tuple2[String, String]]
      // 遍历所有同网吧人员 若是同行人 则添加到list中 对时间格式加约束
      for (i <- 0 until groupPerson.length - 1 if groupPerson(i).getAs[String](DLSJ).length == 14) { // [0,n-2]
        // 记录两人的座位 A B 。。。其他信息需要 在这里添加
        for (j <- i + 1 until groupPerson.length if groupPerson(j).getAs[String](DLSJ).length == 14) {// [i+1,n-1]
        // 判断两个人是否为网吧同行 找出同网吧地址，同登录时间（间隔半个小时以内），同行次数加一
        // 登录时间 20180922211301
          val A_DLSJ = dateToMinutes(groupPerson(i).getAs[String](DLSJ))
          val B_DLSJ = dateToMinutes(groupPerson(j).getAs[String](DLSJ))
          if(math.abs(A_DLSJ - B_DLSJ) <= 30){
            val cogroupPerson = groupPerson(i).getAs[String](SFZH) + "_" + groupPerson(j).getAs[String](SFZH)
            // 拿到本次航班的所有信息 拼接成json串 { 航班号："",离港时间：""}
            val jsonObject = new JSONObject()
            // 添加A和B的座位信息
            for(col <- columns){
              jsonObject.put(col , groupPerson(j).getAs[String](col))
            }
            tuplesList.append((cogroupPerson, jsonObject.toJSONString))//[(a-b,json),(a-c,json)...]
          }
        }
      }
      tuplesList//里面是每个网吧同行的关系
    })
    import scala.collection.mutable
    // （A乘客-B乘客，row） 给阈值
    val resRdd: RDD[Row] = relatedPersonRdd.groupByKey()
      // 先过滤一次 两人总的同行次数应该大于 最小的阈值
      .filter(_._2.size >= math.min(multiBarFilterNum,singleBarFilterNum))
      .map(x => {
        val barList = x._2.toList

        var result = x._1 + "=" + barList.size + "=["
        val barMap = new mutable.HashMap[String,Int]() // map:网吧地址，次数
        for (temp <- barList) {
          // 拿到所有航班的所有信息 拼接成json串数组 [{ 航班号："",离港时间：""},{}]
          result += temp + ","
          // 求出 单个网吧地址同行次数为5的 多个网吧地址同行次数为3的 放入map集合
          val busInfo = JSON.parseObject(temp)
          val busInfoKey = busInfo.getString(WBDZ)
          if(barMap.contains(busInfoKey)){
            barMap.put(busInfoKey,barMap.get(busInfoKey).get + 1)
          }else{
            barMap.put(busInfoKey,1)
          }
        }
        val multiBarCount = barMap.keys.size
        val singleBarCount = barMap.values.max
        // 去掉最后一个 逗号 再加上 ]
        multiBarCount + "=" + singleBarCount + "=" +result.substring(0,result.length-1) + "]"
      })
      .filter(x => {
        // 对同行次数进行过滤
        x.split("=")(0).toInt >= multiBarFilterNum && x.split("=")(1).toInt >= singleBarFilterNum
      })
      .map(x =>{
        val colsTemp = x.split("=")
        Row(colsTemp(0),colsTemp(1),colsTemp(2),colsTemp(3),colsTemp(4))
      })

    val schema: StructType = StructType(Array(
      StructField("multiBarFilterNum",StringType),
      StructField("singleBarFilterNum",StringType),
      StructField("A-ZJHM_B-ZJHM",StringType),
      StructField("TotalCount",StringType),
      StructField("Info",StringType)
    ))
    session.createDataFrame(resRdd,schema)
  }


  /**
    * 将日期转化为分钟
    * @param DLSJ
    * @return
    */
  def dateToMinutes(date : String) : Long = {
    val dateSeconds: Long = new SimpleDateFormat("yyyyMMddHHmmss").parse(date).getTime
    val dateMinute: Long = dateSeconds / 60 /1000
    dateMinute
  }

  /**
    * 客车同行
    * 同行规则：同客车班次（客车发车时间相同），同行次数加一
    * 单客车同行次数不小于5的为同行人
    * 多客车同行次数不小于3的为同行人
    * @param df 数据集
    * @param multiBusFilterNum 多客车同行次数阈值
    * @param singleBusFilterNum 单客车同行次数阈值
    * @param ZJHM 证件号码
    * @param KCBC 客车编号
    * @param KCFCSJ 客车发车时间
    * @param busCols 客车信息列 以分号分隔
    * @return
    */
  def busTravelTwoModel(df:Dataset[Row],multiBusFilterNum:Int,singleBusFilterNum:Int,ZJHM:String,KCBC:String,KCFCSJ:String,busCols:String)={
    // A乘客_B乘客==同行次数==航班号_离港时间,航班号_离港时间,... //
    import org.apache.spark.sql.functions._
    val columns = busCols.split(";")
    val session = df.sparkSession
    val rdd = df.filter(col(ZJHM).isNotNull && col(KCBC).isNotNull && col(KCFCSJ).isNotNull)
      .rdd
    // 转化成（k,v）再 按照 航班号，离港时间 分组
    val kvRdd = rdd.map(x=>(x.getAs(KCBC).toString + "_" +x.getAs(KCFCSJ).toString,x))
    val groupedRDD: RDD[(String, Iterable[Row])] = kvRdd.groupByKey()
    //    groupedRDD.foreach(x=>{println(x._1)})
    // （A乘客-B乘客，row）
    val relatedPersonRdd: RDD[(String, String)] = groupedRDD.flatMap(x => {
      // 对组内所有人按照身份证号 从小到大 排序
      val groupPerson: List[Row] = x._2.toList.sortBy(_.getAs[String](ZJHM))
      // 目标结果集合：（A乘客-B乘客，flightAndTime）
      val tuplesList = new ListBuffer[Tuple2[String, String]]
      // 遍历同行中 每个人
      for (i <- 0 until groupPerson.length - 1) { // [0,n-2]
        // 记录两人的座位 A B 。。。其他信息需要 在这里添加
        for (j <- i + 1 until groupPerson.length) {// [i+1,n-1]
        val cogroupPerson = groupPerson(i).getAs[String](ZJHM) + "_" + groupPerson(j).getAs[String](ZJHM)
          // 拿到本次航班的所有信息 拼接成json串 { 航班号："",离港时间：""}
          val jsonObject = new JSONObject()
          // 添加A和B的座位信息
          for(col <- columns){
            jsonObject.put(col , groupPerson(j).getAs[String](col))
          }
          tuplesList.append((cogroupPerson, jsonObject.toJSONString))
        }
      }
      tuplesList
    })
    import scala.collection.mutable
    // （A乘客-B乘客，flightAndTime） 给阈值
    val resRdd: RDD[Row] = relatedPersonRdd.groupByKey()
      // 先过滤一次 两人总的同行次数应该大于 最小的阈值
      .filter(_._2.size >= math.min(multiBusFilterNum,singleBusFilterNum))
      .map(x => {
        val busList = x._2.toList

        var result = x._1 + "=" + busList.size + "=["
        val busMap = new mutable.HashMap[String,Int]() // map:汽车班次，次数
        for (temp <- busList) {
          // 拿到所有航班的所有信息 拼接成json串数组 [{ 航班号："",离港时间：""},{}]
          result += temp + ","
          // 求出 单客车同行次数 5 多客车同行次数 3 放入map集合
          val busInfo = JSON.parseObject(temp)
          val busInfoKey = busInfo.getString(KCBC)
          if(busMap.contains(busInfoKey)){
            busMap.put(busInfoKey,busMap.get(busInfoKey).get + 1)
          }else{
            busMap.put(busInfoKey,1)
          }
        }
        val multiBusCount = busMap.keys.size
        val singleBusCount = busMap.values.max
        // 去掉最后一个 逗号 再加上 ]
        multiBusCount + "=" + singleBusCount + "=" +result.substring(0,result.length-1) + "]"
      })
      .filter(x => {
        // 对同行次数进行过滤
        x.split("=")(0).toInt >= multiBusFilterNum && x.split("=")(1).toInt >= singleBusFilterNum
      })
      .map(x =>{
        val colsTemp = x.split("=")
        Row(colsTemp(0),colsTemp(1),colsTemp(2),colsTemp(3),colsTemp(4))
      })

    val schema: StructType = StructType(Array(
      StructField("multiBusFilterNum",StringType),
      StructField("singleBusFilterNum",StringType),
      StructField("A-ZJHM_B-ZJHM",StringType),
      StructField("TotalCount",StringType),
      StructField("Info",StringType)
    ))
    session.createDataFrame(resRdd,schema)

  }


  /**
    * 客车邻座
    * 邻座规则：同客车（客车发车时间相同）邻座大于2次的为同行人
    * @param df 数据集
    * @param filterNum 邻座次数阈值 >=
    * @param ZJHM 证件号码
    * @param KCBC 客车编号
    * @param KCFCSJ 客车发车时间
    * @param ZWH 座位号
    * @param busCols 客车信息列 以分号分隔
    * @return
    */
  def busTravelNeighbourModel(df:Dataset[Row],filterNum:Int,ZJHM:String,KCBC:String,KCFCSJ:String,ZWH:String,busCols:String)={
    val session = df.sparkSession
    val columns = busCols.split(";")
    import org.apache.spark.sql.functions._
    val rdd = df.filter(col(ZJHM).isNotNull && col(KCBC).isNotNull && col(KCFCSJ).isNotNull && col(ZWH).isNotNull)
      .rdd
    // 转化成（k,v）再 按照 航班号，离港时间 分组
    val kvRdd = rdd.map(x=>(x.getAs(KCBC).toString + "_" +x.getAs(KCFCSJ).toString,x))
    val groupedRDD: RDD[(String, Iterable[Row])] = kvRdd.groupByKey()
    // （A乘客-B乘客，row）
    val relatedPersonRdd: RDD[(String, String)] = groupedRDD.flatMap(x => {
      // 对组内所有人按照身份证号 从小到大 排序
      val groupPerson: List[Row] = x._2.toList.sortBy(_.getAs[String](ZJHM))
      // 目标结果集合：（A乘客-B乘客，flightAndTime）
      val tuplesList = new ListBuffer[Tuple2[String, String]]
      // 遍历同行中 每个人
      for (i <- 0 until groupPerson.length - 1) { // [0,n-2]
        // 记录两人的座位 A B 。。。其他信息需要 在这里添加
        for (j <- i + 1 until groupPerson.length) {// [i+1,n-1]
        val cogroupPerson = groupPerson(i).getAs[String](ZJHM) + "_" + groupPerson(j).getAs[String](ZJHM)
          // 拿到本次航班的所有信息 拼接成json串 { 航班号："",离港时间：""}
          val jsonObject = new JSONObject()
          // 添加A和B的座位信息
          jsonObject.put("A_" + ZWH,groupPerson(i).getAs[String](ZWH))
          jsonObject.put("B_" + ZWH,groupPerson(j).getAs[String](ZWH))
          for(col <- columns){
            jsonObject.put(col , groupPerson(j).getAs[String](col))
          }
          tuplesList.append((cogroupPerson, jsonObject.toJSONString))
        }
      }
      tuplesList
    })
    // A B A座位 B
    // （A乘客-B乘客，flightAndTime(jsonString) ） 给阈值
    val groupPerson_SeatsDF = relatedPersonRdd.groupByKey()
      // 先过滤一次 两人总的邻座次数应该大于阈值
      .filter(_._2.size >= filterNum)
      .map(x => {
        val flightList: List[String] = x._2.toList
        var result = x._1 + "=" + flightList.size + "=["
        // 邻座次数
        var neighbourCount = 0
        for (temp <- flightList) {
          // 拿到所有航班的所有信息 拼接成json串数组 [{ 航班号："",离港时间：""},{}]
          result += temp + ","
          val busInfoJson: JSONObject = JSON.parseObject(temp)
          //        val tempJsonObj: JSONObject = JSON.parseObject(temp)
          val seatA = busInfoJson.getString("A_" + ZWH)
          val seatB = busInfoJson.getString("B_" + ZWH)
          if(seatA != null && seatB != null){
            // 判断两个人 是否邻座 有三连坐 二连坐的情况 所以这里只判断座位号之差绝对值为1
            if(math.abs(seatA.toInt - seatB.toInt)== 1)
              neighbourCount += 1
          }

        }
        // 去掉最后一个 逗号 再加上 ]
        neighbourCount + "=" + result.substring(0,result.length-1) + "]"
        // A乘客_B乘客==同行次数==航班号_离港时间_A座位号_B座位号,航班号_离港时间_A座位号_B座位号,...
      })
      .filter(_.split("=")(0).toInt >= filterNum)
      .map(x => {
        val colsTemp = x.split("=")
        Row(colsTemp(0),colsTemp(1),colsTemp(2),colsTemp(3))
      })

    val schema: StructType = StructType(Array(
      StructField("NeighbourTimes",StringType),
      StructField("A-ZJHM_B-ZJHM",StringType),
      StructField("TotalCount",StringType),
      StructField("Info",StringType)
    ))
    session.createDataFrame(groupPerson_SeatsDF,schema)

  }


  /**
    * 两人飞机同行统计
    * 原始数据：证件号，航班号，离港时间
    * 1 转化成（航班号_离港时间,row）再按key分组
    * 2 组内，先对所有乘客按身份证号排序 再两重循环遍历所有人生成A-B同行对 再压平得到（A_B,航班号_离港时间）
    * 3 按key分组，对组内数量即同行次数按照阀值过滤，再把组内信息拼接成字符串
    * 注：已对参数字段进行去空操作
    * @param df
    * @param filterNum 同行次数阀值 >=
    * @param ZJHM 证件号码
    * @param HBH 航班号
    * @param LG_RQ 离港时间
    * @param flightCols 航班信息列，以分号分隔
    * @return
    */
  def flightTravelTwoModel(df:Dataset[Row],filterNum:Int,ZJHM:String,HBH:String,LG_RQ:String,flightCols:String)={
    // A乘客_B乘客==同行次数==航班号_离港时间,航班号_离港时间,... //
    import org.apache.spark.sql.functions._
    val columns = flightCols.split(";")
    val session = df.sparkSession
    val rdd = df.filter(col(ZJHM).isNotNull && col(HBH).isNotNull && col(LG_RQ).isNotNull)
      .rdd
    // 转化成（k,v）再 按照 航班号，离港时间 分组
    val kvRdd = rdd.map(x=>(x.getAs(HBH).toString + "_" +x.getAs(LG_RQ).toString,x))
    val groupedRDD: RDD[(String, Iterable[Row])] = kvRdd.groupByKey()
    //    groupedRDD.foreach(x=>{println(x._1)})
    // （A乘客-B乘客，flightAndTime）
    val relatedPersonRdd: RDD[(String, String)] = groupedRDD.flatMap(x => {
      val flightAndTime: String = x._1
      // 对组内所有人按照身份证号 从小到大 排序
      val groupPerson: List[Row] = x._2.toList.sortBy(_.getAs[String](ZJHM))
      // 目标结果集合：（A乘客-B乘客，flightAndTime）
      val tuplesList = new ListBuffer[Tuple2[String, String]]
      // 遍历同行中 每个人
      for (i <- 0 until groupPerson.length - 1) { // [0,n-2]
        // 记录两人的座位 A B 。。。其他信息需要 在这里添加
        for (j <- i + 1 until groupPerson.length) {// [i+1,n-1]
        val cogroupPerson = groupPerson(i).getAs[String](ZJHM) + "_" + groupPerson(j).getAs[String](ZJHM)
          // 拿到本次航班的所有信息 拼接成json串 { 航班号："",离港时间：""}
          val jsonObject = new JSONObject();
          // 添加A和B的座位信息
          for(col <- columns){
            jsonObject.put(col , groupPerson(j).getAs[String](col))
          }
          tuplesList.append((cogroupPerson, jsonObject.toJSONString))
        }
      }
      tuplesList
    })
    // （A乘客-B乘客，flightAndTime） 给阈值
    val resRdd: RDD[Row] = relatedPersonRdd.groupByKey()
      // 给阈值
      .filter(_._2.size >= filterNum)
      .map(x => {
        val flightList = x._2.toList
        var result = x._1 + "=" + flightList.size + "=["
        for (temp <- flightList) {
          // 拿到所有航班的所有信息 拼接成json串数组 [{ 航班号："",离港时间：""},{}]
          result += temp + ","
        }
        // 去掉最后一个 逗号 再加上 ]
        result.substring(0,result.length-1) + "]"
      }).map(x =>{
      val colsTemp = x.split("=")
      Row(colsTemp(0),colsTemp(1),colsTemp(2))
    })

    val schema: StructType = StructType(Array(
      StructField("A-ZJHM_B-ZJHM",StringType),
      StructField("TogetherTimes",StringType),
      StructField("Info",StringType)
    ))
    session.createDataFrame(resRdd,schema)
  }


  /**
    * 两人飞机邻座统计
    * 在同行统计结果的基础上，即 A乘客_B乘客==同行次数==航班号_离港时间_A座位号_B座位号,航班号_离港时间_A座位号_B座位号,...
    * 进行map操作，统计出A和B邻座的次数，再按照阀值过滤
    * 得到结果：邻座次数==A乘客_B乘客==同行次数==航班号_离港时间_A座位号_B座位号,航班号_离港时间_A座位号_B座位号,...
    * 注：已对参数字段进行去空操作
    * @param df
    * @param filterNum 过滤邻座次数的阀值 >=
    * @param ZJHM 证件号码
    * @param HBH 航班号
    * @param LG_RQ 离港时间
    * @param LKZW 旅客座位号 如 23B仅和23A、23C相邻
    * @param flightCols 航班信息列，以分号分隔
    * @return
    */
  def flightTravelNeighbourModel(df:Dataset[Row],filterNum:Int,ZJHM:String,HBH:String,LG_RQ:String,LKZW:String,flightCols:String)={
    val session = df.sparkSession
    val columns = flightCols.split(";")
    import org.apache.spark.sql.functions._
    val rdd = df.filter(col(ZJHM).isNotNull && col(HBH).isNotNull && col(LG_RQ).isNotNull && col(LKZW).isNotNull)
      .rdd
    // 转化成（k,v）再 按照 航班号，离港时间 分组
    val kvRdd = rdd.map(x=>(x.getAs(HBH).toString + "_" +x.getAs(LG_RQ).toString,x))
    val groupedRDD: RDD[(String, Iterable[Row])] = kvRdd.groupByKey()
    // （A乘客-B乘客，flightAndTime）
    val relatedPersonRdd: RDD[(String, String)] = groupedRDD.flatMap(x => {
      val flightAndTime: String = x._1
      // 对组内所有人按照身份证号 从小到大 排序
      val groupPerson: List[Row] = x._2.toList.sortBy(_.getAs[String](ZJHM))
      // 目标结果集合：（A乘客-B乘客，flightAndTime）
      val tuplesList = new ListBuffer[Tuple2[String, String]]
      // 遍历同行中 每个人
      for (i <- 0 until groupPerson.length - 1) { // [0,n-2]
        // 记录两人的座位 A B 。。。其他信息需要 在这里添加
        for (j <- i + 1 until groupPerson.length) {// [i+1,n-1]
        val cogroupPerson = groupPerson(i).getAs[String](ZJHM) + "_" + groupPerson(j).getAs[String](ZJHM)
          // 拿到本次航班的所有信息 拼接成json串 { 航班号："",离港时间：""}
          val jsonObject = new JSONObject();
          // 添加A和B的座位信息
          jsonObject.put("A_" + LKZW,groupPerson(i).getAs[String](LKZW))
          jsonObject.put("B_" + LKZW,groupPerson(j).getAs[String](LKZW))
          for(col <- columns){
            jsonObject.put(col , groupPerson(j).getAs[String](col))
          }
          tuplesList.append((cogroupPerson, jsonObject.toJSONString))
        }
      }
      tuplesList
    })
    // A B A座位 B
    // （A乘客-B乘客，flightAndTime(jsonString) ） 给阈值
    val groupPerson_SeatsDF = relatedPersonRdd.groupByKey().filter(_._2.size >= filterNum).map(x => {
      val flightList: List[String] = x._2.toList
      var result = x._1 + "=" + flightList.size + "=["
      //      val jsonArr = new JSONArray()
      for (temp <- flightList) {
        // 拿到所有航班的所有信息 拼接成json串数组 [{ 航班号："",离港时间：""},{}]
        result += temp + ","
      }
      // 去掉最后一个 逗号 再加上 ]
      result.substring(0,result.length-1) + "]"
      // A乘客_B乘客==同行次数==航班号_离港时间_A座位号_B座位号,航班号_离港时间_A座位号_B座位号,...
    })

    val resRdd = groupPerson_SeatsDF.map(x=>{

      val strings = x.split("=")
      // 乘客证件号
      val passageInfo = strings(0)
      // json数组 解析1
      val allInfo = strings(2)
      val array = JSON.parseArray(allInfo)
      var count = 0
      // 遍历二人所有同行过的航班 再判断是否都是邻座
      // [{ 航班号："",离港时间：""},{}]
      for (i <- 0 until array.size()){
        val temp: JSONObject = array.getJSONObject(i)
        //        val tempJsonObj: JSONObject = JSON.parseObject(temp)
        val seatA = temp.getString("A_" + LKZW)
        val seatB = temp.getString("B_" + LKZW)
        if(seatA != null && seatB != null){
          val seatA_Row = seatA.substring(0,seatA.length - 1)
          val seatB_Row = seatB.substring(0,seatB.length - 1)
          // 判断两个人 是否同排 是否邻座
          if(seatA_Row.equals(seatB_Row) && math.abs(seatA.charAt(seatA.length-1) - seatB.charAt(seatB.length-1)) == 1)
            count += 1
        }
      }
      count + "=" + x
    }).filter(_.split("=")(0).toInt >= filterNum)
      .map(x => {
        val colsTemp = x.split("=")
        Row(colsTemp(1),colsTemp(0),colsTemp(2),colsTemp(3))
      })

    val schema: StructType = StructType(Array(
      StructField("A-ZJHM_B-ZJHM",StringType),
      StructField("NeighbourTimes",StringType),
      StructField("TogetherTimes",StringType),
      StructField("Info",StringType)
    ))
    session.createDataFrame(resRdd,schema)
  }

}
