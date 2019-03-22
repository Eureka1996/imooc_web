package com.wufuqiang.spark.web;

import com.wufuqiang.dao.CourseClickCountDAO;
import com.wufuqiang.domain.CourseClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.* ;

@RestController
public class ImoocStatApp {
    private static Map<String,String> courses = new HashMap<>() ;
    static{
        courses.put("112","SparSQL慕课网日志分析");
        courses.put("130","10小时入门大数据");
        courses.put("131","深度学习之神经网络核心原理与算法");
        courses.put("145","强大的Node.js在Web开发的应用");
        courses.put("150","Vue+Django实战");
        courses.put("163","Web前端性能优化");
        courses.put("246","Java基础课程");
        courses.put("774","Python的高阶开发");
    }

    @Autowired
    CourseClickCountDAO courseClickCountDAO ;

/*
    @RequestMapping(value="/course_clickcount",method= RequestMethod.GET)
    public ModelAndView courseClickCount() throws Exception{
        ModelAndView view = new ModelAndView("demo") ;
        List<CourseClickCount> list = courseClickCountDAO.query("20190322") ;

        for(CourseClickCount model : list){
            model.setName(courses.get(model.getName().substring(9)));
            System.out.println(model.getName()+":"+model.getValue()) ;
        }
        JSONArray json = JSONArray.fromObject(list) ;
        view.addObject("data_json",json) ;
        return view ;
    }
*/

    @RequestMapping(value="/course_clickcount",method= RequestMethod.POST)
    @ResponseBody
    public List<CourseClickCount> courseClickCount() throws Exception{
        List<CourseClickCount> list = courseClickCountDAO.query("20190322") ;

        for(CourseClickCount model : list){
            model.setName(courses.get(model.getName().substring(9)));
            System.out.println(model.getName()+":"+model.getValue()) ;
        }
        return list ;
    }

    @RequestMapping(value = "/echarts", method = RequestMethod.GET)
    public ModelAndView echarts(){
        return new ModelAndView("echarts");
    }
}
