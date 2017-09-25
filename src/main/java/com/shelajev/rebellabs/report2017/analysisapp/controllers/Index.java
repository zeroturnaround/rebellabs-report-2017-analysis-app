package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

@Controller
@RequestMapping("/")
public class Index {

  private RequestMappingHandlerMapping mapping;
  private Set<String> skipTheseLinks;

  public Index(RequestMappingHandlerMapping mapping) {
    this.mapping = mapping;

    skipTheseLinks = new HashSet<>();
    skipTheseLinks.add("/");
    skipTheseLinks.add("/error");
  }

  @RequestMapping("/")
  public String index(Model model) {
    Set<RequestMappingInfo> req = mapping.getHandlerMethods().keySet();
    TreeSet<String> links = new TreeSet<>();
    req.stream().forEach(info -> {
      String url = info.getPatternsCondition().getPatterns().iterator().next();
      if(!skipTheseLinks.contains(url)) {
        links.add(url);
      }
    });
    model.addAttribute( "endPoints", links);
    return "index";
  }



}
