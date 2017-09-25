package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

import static com.shelajev.rebellabs.report2017.analysisapp.model.tables.Results.RESULTS;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.count;

@RestController
@RequestMapping("/company/")
public class ByCompanyStats {

  private static final Logger log = LoggerFactory.getLogger(ByCompanyStats.class);

  private final DSLContext dsl;

  public ByCompanyStats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/byCompany")
  public Map<?, ?> statsByCompany() {
    Result<Record6<String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal>> records = dsl
      .select(RESULTS.COMPANY,
        avg(RESULTS.IDE_RATING).as("ide-rating"),
        avg(RESULTS.LANGUAGE_RATING).as("language-rating"),
        avg(RESULTS.APPSTACK_RATING).as("appstack-rating"),
        avg(RESULTS.ARCHITECTURE_RATING).as("architecture-rating"),
        avg(RESULTS.DATABASE_RATING).as("database-rating"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull())
      .groupBy(RESULTS.COMPANY)
      .orderBy(DSL.val(1).desc())
      .fetch();
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();

    records.forEach(r6 -> {
      result.put(r6.value1(), r6.intoMap());
    });
    return result;
  }

  @RequestMapping("/countByCompanyAndIDE")
  public LinkedHashMap<String, List<Map<String, Object>>> countByCompanyAndIDE() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.COMPANY, RESULTS.IDE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull()).and(RESULTS.IDE.isNotNull())
      .groupBy(RESULTS.COMPANY, RESULTS.IDE)
      .orderBy(DSL.val(3).desc())
      .fetch();
    LinkedHashMap<String, List<Map<String,Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put("ide", r.value2());
      props.put("count", r.value3());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    return result;
  }

  @RequestMapping("/countByCompanyAndArchitecture")
  public LinkedHashMap<String, List<Map<String, Object>>> countByCompanyAndArchitecture() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.COMPANY, RESULTS.ARCHITECTURE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull()).and(RESULTS.ARCHITECTURE.isNotNull())
      .groupBy(RESULTS.COMPANY, RESULTS.ARCHITECTURE)
      .orderBy(DSL.val(3).desc())
      .fetch();
    LinkedHashMap<String, List<Map<String,Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put("architecture", r.value2());
      props.put("count", r.value3());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    return result;
  }

  @RequestMapping("/countByCompanyAppstack")
  public LinkedHashMap<String, List<Map<String, Object>>> countByCompanyAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.COMPANY, RESULTS.APPSTACK, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull()).and(RESULTS.APPSTACK.isNotNull())
      .groupBy(RESULTS.COMPANY, RESULTS.APPSTACK)
      .orderBy(DSL.val(3).desc())
      .fetch();
    LinkedHashMap<String, List<Map<String,Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put("appstack", r.value2());
      props.put("count", r.value3());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    return result;
  }

  @RequestMapping("/countByCompanyDevops")
  public LinkedHashMap<String, List<Map<String, Object>>> countByCompanyDevops() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.COMPANY, RESULTS.DEVOPS, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull()).and(RESULTS.DEVOPS.isNotNull())
      .groupBy(RESULTS.COMPANY, RESULTS.DEVOPS)
      .orderBy(DSL.val(3).desc())
      .fetch();
    LinkedHashMap<String, List<Map<String,Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put("devops", r.value2());
      props.put("count", r.value3());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    return result;
  }

  @RequestMapping("/countByCompanyPerformance")
  public LinkedHashMap<String, List<Map<String, Object>>> countByCompanyPerformance() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.COMPANY, RESULTS.PERFORMANCE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull()).and(RESULTS.PERFORMANCE.isNotNull())
      .groupBy(RESULTS.COMPANY, RESULTS.PERFORMANCE)
      .orderBy(DSL.val(3).desc())
      .fetch();
    LinkedHashMap<String, List<Map<String,Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put("performance", r.value2());
      props.put("count", r.value3());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    return result;
  }
}

