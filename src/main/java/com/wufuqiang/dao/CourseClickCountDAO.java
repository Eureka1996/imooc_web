package com.wufuqiang.dao;

import com.wufuqiang.domain.CourseClickCount;
import com.wufuqiang.utils.HBaseUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



@Component
public class CourseClickCountDAO {
    public List<CourseClickCount> query(String day)throws Exception{
      List<CourseClickCount> list = new ArrayList<>() ;

      Map<String,Long> map = HBaseUtil.query("imooc_course_clickcount",day) ;

      for(Map.Entry<String,Long> entry:map.entrySet()){
          list.add(new CourseClickCount(entry.getKey(),entry.getValue())) ;
      }
      return list ;
    }

    public static void main(String[]args)throws Exception{
        CourseClickCountDAO ccc = new CourseClickCountDAO() ;
        List<CourseClickCount> result = ccc.query("20190322") ;
        for(CourseClickCount c : result){
            System.out.println(c.getName()+":" +c.getValue()) ;
        }
    }
}
