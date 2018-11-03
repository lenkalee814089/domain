package com.dataexa.insight.domain.security.spark.dataset.util;


import com.alibaba.fastjson.JSONObject;
import com.dataexa.insight.domain.security.spark.dataset.model.RailwayRecord;
import org.apache.log4j.Logger;

import java.util.*;

public class RailwayFindUtils {
    private static final Logger LOGGER = Logger.getLogger(RailwayFindUtils.class);

    public static boolean isLetter(String s){
        char c = s.charAt(0);
        if (c>='a'&&c<='z'){
            return true;
        }
        return false;
    }

    /**
     * 判断两座位号是否邻座
     * @param a
     * @param b
     * @return
     */
    public static boolean isCloseSeatNUm(String a, String b){
        int aExceptLast=Integer.parseInt(a.substring(0, a.length()-1)) ;
        int bExceptLast=Integer.parseInt(b.substring(0, b.length()-1));
        String aLast=a.toLowerCase().substring(a.length()-1, a.length());
        String bLast=b.toLowerCase().substring(b.length()-1, b.length());
        //两座位号尾数都是字母时
        if ((aExceptLast==bExceptLast)&& isLetter(aLast)&&isLetter(bLast)){
            switch (aLast) {
                //动车座位没有e
                case "a":return bLast.equals("b") ;
                case "b":return bLast.equals("a")||bLast.equals("c");
                case "c":return bLast.equals("b")||bLast.equals("d") ;
                case "d":return bLast.equals("c")||bLast.equals("f") ;
                case "f":return bLast.equals("d") ;
                default:
                    System.out.println("判断是否邻座时出现错误,如果尾字母为e,请修正测试数据或忽略本条信息!    "+a+"   "+b);
            }

            return false;
            //都是数字时
        }else if ((!isLetter(aLast))&&(!isLetter(bLast))){
            int bigSeatNum = Math.max(Integer.parseInt(a), Integer.parseInt(b)) ;
            int miniSeatNum = Math.min(Integer.parseInt(a), Integer.parseInt(b)) ;
            if (bigSeatNum-miniSeatNum==1){
                if (bigSeatNum%10==0||bigSeatNum%10==5||miniSeatNum%10==4||miniSeatNum%10==9){
                    return false;
                }
                return true;
            }
        }
        //一个是数字一个是字母,false
        return false;


    }

    public static void main(String[] args) {
        System.out.println(isCloseSeatNUm("439", "455"));
    }

    /**
     * 只判断两条记录座位是否相邻
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean isCloseSeat(RailwayRecord recordA, RailwayRecord recordB){
        if (recordA.getCXH().equals(recordB.getCXH())&&isCloseSeatNUm(recordA.getZWH(), recordB.getZWH()) ){
            return true;
        }
        return false;
    }

    /**
     * 判断是否同旅程,即,同出发时间,同始发地,同目的地,同车次
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean isSameRailway(RailwayRecord recordA, RailwayRecord recordB){
        return recordA.getCC().equals(recordB.getCC()) && recordA.getFCSJ().equals(recordB.getFCSJ())&&
                recordA.getMDD().equals(recordB.getMDD())&&recordA.getSFD().equals(recordB.getSFD());
    }
    /**
     * 返回一个数组,里面是两个list,第一个放两个人判定为"同行"的消息json,第二个放判定为"邻座"的信息json
     * @param records
     * @return
     */
    public static Map<String,List<JSONObject> []> getResultMap(List<RailwayRecord> records){
        Map<String, List<JSONObject> []> map = new HashMap<>();

        List<JSONObject>[] arr ;
        String key;
        RailwayRecord recordA ;
        RailwayRecord recordB ;
        Iterator<RailwayRecord> it2;
        Iterator<RailwayRecord> it = records.iterator();

        while (it.hasNext()) {
            recordA=it.next();
            //把被判断的记录从集合移除，提高效率
            it.remove();
            it2 = records.iterator();
            while (it2.hasNext()) {
                recordB = it2.next();
                //同一人的记录不要进行判定
                if(!recordA.getGMSFHM().equals(recordB.getGMSFHM())){

                    //相似判断：如果某两条记录判定同行
                    if (isSameRailway(recordA, recordB)){

                        key = StringUtil.combine2GMSFZ(recordA.getGMSFHM()  ,recordB.getGMSFHM() );
                        //获得同行数和邻座数
                        arr = map.getOrDefault(key,  new List[]{new LinkedList(),new LinkedList(),new LinkedList(),new LinkedList()});
                        arr[0].add(recordA.getDiffFieldJsonObject());
                        arr[0].add(recordB.getDiffFieldJsonObject());
                        arr[2].add(recordA.getSameFieldJsonObject());
                        if (isCloseSeat(recordA, recordB)){

                            arr[1].add(recordA.getDiffFieldJsonObject());
                            arr[1].add(recordB.getDiffFieldJsonObject());
                            arr[3].add(recordA.getSameFieldJsonObject());

                        }
                        map.put(key, arr);
                    }
                }
            }
        }
        return map;
    }

    public static Comparator<String> getComparetor(){
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String[] s1 = o1.split(":");
                String[] s2 = o2.split(":");
                return Integer.compare(Integer.parseInt(s1[0]), Integer.parseInt(s2[0]));
            }
        };
        return comparator;
    }

}
