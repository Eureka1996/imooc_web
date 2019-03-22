package com.wufuqiang.spark.web;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

/**
 * 第一个Boot应用
 */

@RestController
public class HelloBoot {

    @RequestMapping(value="/hello",method= RequestMethod.GET)
    public String sayHello(){
        return "Hello World Spring Boot" ;
    }

    @RequestMapping(value="/firstchart",method= RequestMethod.GET)
    public ModelAndView firstDemo(){
        return new ModelAndView("demo") ;
    }
}
