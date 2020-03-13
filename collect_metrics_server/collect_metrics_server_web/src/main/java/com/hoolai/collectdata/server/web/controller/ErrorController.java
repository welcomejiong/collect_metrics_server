package com.hoolai.collectdata.server.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class ErrorController extends AbstractPanelController{

	@RequestMapping(value = "/404", method = { RequestMethod.GET,RequestMethod.POST })
	public String to404(Model model)throws Exception {
		
		return "error/404";
	}
	
	@RequestMapping(value = "/50x", method = { RequestMethod.GET,RequestMethod.POST })
	public String to50x(Model model)throws Exception {
		
		return "error/50x";
	}
	
	
}

