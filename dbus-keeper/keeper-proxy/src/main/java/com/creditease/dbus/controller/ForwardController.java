package com.creditease.dbus.controller;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by Administrator on 2018/8/8.
 */
@Controller
public class ForwardController implements ErrorController {

    private static final String ERROR_PATH = "/error";

    @RequestMapping(value = ERROR_PATH)
    public String handleError() {
        return "index.html";
    }

    @Override
    public String getErrorPath() {
        return ERROR_PATH;
    }

}
