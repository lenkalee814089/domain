package com.dataexa.insight.domain.security.spark.dataset.util;


import com.alibaba.fastjson.JSONObject;
import com.dataexa.insight.domain.security.spark.dataset.model.HotelRecord;

import java.util.*;

public class HotelFindUtils {

    /**
     * 两条记录是否行迹相似
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean ifSimilarRecord(HotelRecord recordA, HotelRecord recordB){
        long recordATfsj = recordA.getLeaveTimeMillSecond();
        long recordAStayLong = recordA.getStayMillSecond();

        long recordBRzsj = recordB.getComeTimeMillSecond();
        long recordBStayLong = recordB.getStayMillSecond();


        long togetherTime = Math.abs(recordATfsj - recordBRzsj)  ;
        if ((togetherTime/ ((double) recordAStayLong))>=0.8&&(togetherTime/ ((double) recordBStayLong)>=0.8)){
            return true;
        }else {
            return false;
        }


    }

    /**
     * 获取任意两公民在某个旅馆的行踪相似度和同住宿次数
     * @param records
     * @return
     */
    public static Map<String, List<JSONObject> []> getResultMap(List<HotelRecord> records){
        Map<String, List<JSONObject> []> map = new HashMap<>();
        List<JSONObject>[] arr;
        String key;
        HotelRecord recordA ;
        HotelRecord recordB ;
        Iterator<HotelRecord> it2;
        Iterator<HotelRecord> it1 = records.iterator();
        while (it1.hasNext()) {
            recordA=it1.next();
            //把被判断的记录从集合移除，提高效率
            it1.remove();
            it2 = records.iterator();
            while (it2.hasNext()) {
                recordB = it2.next();
                //同一人的记录不要进行判定
                if(!recordA.getZjhm().equals(recordB.getZjhm())){

                    //相似判断：如果某两条记录判定相似，则把记录对应两个所属人的相似度+1
                    if(ifSimilarRecord(recordA, recordB)){

                        key= StringUtil.combine2Zjhm(recordA.getZjhm(), recordB.getZjhm());
                        arr =map.getOrDefault(key,new List[]{new LinkedList(),new LinkedList(),new LinkedList(),new LinkedList()});
                        arr[0].add(recordA.getDiffFieldJsonObject());
                        arr[0].add(recordB.getDiffFieldJsonObject());
                        arr[2].add(recordA.getSameFieldJsonObject());

                        //同宿判断：
                        if(isSleeptogether(recordA, recordB)){
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



    /**
     * 是否时间交集
     * @param a
     * @param b
     * @return
     */
    public static boolean isIntersect(HotelRecord a,HotelRecord b){
       return !(a.getComeTimeMillSecond()>b.getLeaveTimeMillSecond()||b.getComeTimeMillSecond()>a.getLeaveTimeMillSecond()) ;
    }

    /**
     * 判断两条记录是否同宿
     * @param recordA
     * @param recordB
     * @return
     */
    public static boolean isSleeptogether(HotelRecord recordA,HotelRecord recordB){

        return isIntersect(recordA, recordB)&&recordA.getrzfh().equals(recordB.getrzfh());

    }

    public static Comparator<String> getComparetor(){
        return new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String[] s1 = o1.split(":");
                String[] s2 = o2.split(":");
                return Integer.compare(Integer.parseInt(s1[0]), Integer.parseInt(s1[0]));
            }
        };

    }

}
