package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

import static com.shelajev.rebellabs.report2017.analysisapp.math.Misc.ptc;
import static com.shelajev.rebellabs.report2017.analysisapp.math.Misc.toPtc;
import static com.shelajev.rebellabs.report2017.analysisapp.model.tables.Results.RESULTS;
import static org.jooq.impl.DSL.count;

@RestController
@RequestMapping("/r/")
public class RandomStats {

  private static final Logger log = LoggerFactory.getLogger(RandomStats.class);

  private final DSLContext dsl;

  public RandomStats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/countries")
  public Map<?, ?> countries() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.COUNTRY, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.COUNTRY)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.COUNTRY.getName(), "cnt");
    map.remove(null);
    Map<String, Object> results = new LinkedHashMap<>();
    map.forEach((k, v) -> results.put(k, ptc(v)));
    return results;
  }

  @RequestMapping("/companies")
  public Map<?, ?> companies() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.COMPANY, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.COMPANY)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.COMPANY.getName(), "cnt");
    map.remove(null);
    Map<String, Object> results = new LinkedHashMap<>();
    map.forEach((k, v) -> results.put(k, ptc(v)));
    return results;
  }

  @RequestMapping("/positions")
  public Map<?, ?> positions() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.JOB_DESCRIPTION, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.JOB_DESCRIPTION)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.JOB_DESCRIPTION.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/teamsize")
  public Map<?, ?> teamsize() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.TEAMSIZE, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.TEAMSIZE)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.TEAMSIZE.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/experience")
  public Map<?, ?> experience() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.EXPERIENCE_YEARS, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.EXPERIENCE_YEARS)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.EXPERIENCE_YEARS.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/devops")
  public Map<?, ?> devops() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.DEVOPS, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.DEVOPS)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.DEVOPS.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/devops-reasons")
  public Map<?, ?> devopsReasons() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.DEVOPS_REASON, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.DEVOPS_REASON)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.DEVOPS_REASON.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/performance")
  public Map<?, ?> performance() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.PERFORMANCE, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.PERFORMANCE)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.PERFORMANCE.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/performance-reasons")
  public Map<?, ?> performanceReasons() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.PERFORMANCE_REASON, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.PERFORMANCE_REASON)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.PERFORMANCE_REASON.getName(), "cnt");
    map.remove(null);
    return toPtc(map);
  }

  @RequestMapping("/boost")
  public Map<?, ?> boost() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.PRODUCTIVITY_BOOST, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.PRODUCTIVITY_BOOST)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.PRODUCTIVITY_BOOST.getName(), "cnt");
    map.remove(null);
    Map<String, Object> results = new LinkedHashMap<>();
    map.forEach((k, v) -> results.put(k, ptc(v)));
    return results;
  }

  @RequestMapping("/happiness")
  public Map<?, ?> happiness() {
    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.TECH_CHOICES, count().as("cnt"))
      .from(RESULTS).groupBy(RESULTS.TECH_CHOICES)
      .orderBy(DSL.val(2).desc())
      .fetch().intoMap(RESULTS.TECH_CHOICES.getName(), "cnt");
    map.remove(null);
    Map<String, Double> results = new LinkedHashMap<>();
    map.forEach((k, v) -> {
      if (results.containsKey(k)) {
        results.put(k, results.get(k) + v);
      }
      else {
        results.put(k, Double.valueOf(v));
      }
    });
    results.forEach((k, v) -> results.put(k, ptc(v)));
    return results;
  }

  @RequestMapping("/exiting-tech")
  public Map<?, ?> exitingTech() {
    HashMap<String, String> synonims = new HashMap<>();
    synonims.put("springboot2", "springboot");
    synonims.put("spring-boot", "springboot");
    synonims.put("spring", "spring5");
    synonims.put("javaee", "javaee8");
    synonims.put("javaee7", "javaee8");
    synonims.put("jee7", "javaee8");
    synonims.put("jee", "javaee8");
    synonims.put("angular4", "angular");
    synonims.put("angular2", "angular");
    synonims.put("angular3", "angular");

    Map<String, Integer> map = (Map<String, Integer>) dsl
      .select(RESULTS.EXCITING_TECH, count().as("cnt"))
      .from(RESULTS)
      .groupBy(RESULTS.EXCITING_TECH)
      .orderBy(DSL.val(2).desc()).fetch().intoMap(RESULTS.EXCITING_TECH.getName(), "cnt");
    map.remove(null);
    Map<String, Integer> transformed = new LinkedHashMap<>();
    map.forEach((k, v) -> {
      String[] keys = k.trim().toLowerCase().replaceAll(" ", "").split("[;,./&\\+]");
      for (String key : keys) {
        if (synonims.containsKey(key)) {
          key = synonims.get(key);
        }

        if (transformed.containsKey(key)) {
          transformed.put(key, transformed.get(key) + v);
        }
        else {
          transformed.put(key, v);
        }
      }
    });

//    TreeMap<String, Integer> result = new TreeMap<>(Comparator.comparingDouble(transformed::get).reversed());
//
//    result.putAll(transformed);

    return transformed;
  }

}

