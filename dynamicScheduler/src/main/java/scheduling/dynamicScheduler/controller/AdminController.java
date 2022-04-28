package scheduling.dynamicScheduler.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;
import scheduling.dynamicScheduler.dynamic.DynamicChangeScheduler;

import java.util.HashMap;

@Controller
public class AdminController {

    @Autowired
    DynamicChangeScheduler ps;

    @RequestMapping(value="/setting.do")
    public ModelAndView setting() throws Exception{
        ModelAndView mv = new ModelAndView();
        mv.setViewName("admin/setting");
        return mv;
    }

    @RequestMapping(value="/updateScheduler.do")
    public @ResponseBody
    HashMap<Object, Object> updateScheduler(@RequestParam HashMap<Object, Object> params) throws Exception{
        ps.stopScheduler();
        Thread.sleep(1000);
        ps.changeCronSet((String) params.get("cron"));
        ps.startScheduler();

        HashMap<Object, Object> res = new HashMap<Object, Object>();
        res.put("res", "success");
        return res;
    }

    @RequestMapping(value="/pauseScheduler.do")
    public @ResponseBody HashMap<Object, Object> pauseScheduler(@RequestParam  HashMap<Object, Object> params) throws Exception{
        ps.stopScheduler();
        HashMap<Object, Object> res = new HashMap<Object, Object>();
        res.put("res", "success");
        return res;
    }
}