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
@RequestMapping("/teamsize/")
public class ByTeamsizeStats {

  private static final Logger log = LoggerFactory.getLogger(ByTeamsizeStats.class);

  private final DSLContext dsl;

  public ByTeamsizeStats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/byTeamsize")
  public Map<?, ?> statsByTeamsize() {
    Result<Record6<String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal>> records = dsl
      .select(RESULTS.TEAMSIZE,
        avg(RESULTS.IDE_RATING).as("ide-rating"),
        avg(RESULTS.LANGUAGE_RATING).as("language-rating"),
        avg(RESULTS.APPSTACK_RATING).as("appstack-rating"),
        avg(RESULTS.ARCHITECTURE_RATING).as("architecture-rating"),
        avg(RESULTS.DATABASE_RATING).as("database-rating"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull())
      .groupBy(RESULTS.TEAMSIZE)
      .orderBy(DSL.val(1).desc())
      .fetch();
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();

    records.forEach(r6 -> {
      result.put(r6.value1(), r6.intoMap());
    });
    return result;
  }

  @RequestMapping("/countByTeamsizeAndIDE")
  public LinkedHashMap<String, List<Map<String, Object>>> countByTeamsizeAndIDE() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE, RESULTS.IDE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull()).and(RESULTS.IDE.isNotNull())
      .groupBy(RESULTS.TEAMSIZE, RESULTS.IDE)
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

  @RequestMapping("/countByTeamsizeAndArchitecture")
  public LinkedHashMap<String, List<Map<String, Object>>> countByTeamsizeAndArchitecture() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE, RESULTS.ARCHITECTURE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull()).and(RESULTS.ARCHITECTURE.isNotNull())
      .groupBy(RESULTS.TEAMSIZE, RESULTS.ARCHITECTURE)
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

  @RequestMapping("/countByTeamsizeAppstack")
  public LinkedHashMap<String, List<Map<String, Object>>> countByTeamsizeAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE, RESULTS.APPSTACK, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull()).and(RESULTS.APPSTACK.isNotNull())
      .groupBy(RESULTS.TEAMSIZE, RESULTS.APPSTACK)
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

  @RequestMapping("/countByTeamsizeDevops")
  public LinkedHashMap<String, List<Map<String, Object>>> countByTeamsizeDevops() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE, RESULTS.DEVOPS, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull()).and(RESULTS.DEVOPS.isNotNull())
      .groupBy(RESULTS.TEAMSIZE, RESULTS.DEVOPS)
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

  @RequestMapping("/countByTeamsizePerformance")
  public LinkedHashMap<String, List<Map<String, Object>>> countByTeamsizePerformance() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE, RESULTS.PERFORMANCE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull()).and(RESULTS.PERFORMANCE.isNotNull())
      .groupBy(RESULTS.TEAMSIZE, RESULTS.PERFORMANCE)
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

