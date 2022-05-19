package com.demo.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDate {
    public static String getSysDate(){
        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        // new Date()为获取当前系统时间
        return df.format(new Date());
    }
}
