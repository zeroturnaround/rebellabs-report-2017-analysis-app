package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import com.shelajev.rebellabs.report2017.analysisapp.math.Misc;
import org.jooq.*;
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
@RequestMapping("/s/")
public class Stats {

  private static final Logger log = LoggerFactory.getLogger(Stats.class);

  private final DSLContext dsl;

  public Stats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/stats")
  public Map<String, Object> stats() {
    Integer count = dsl.select(count()).from(RESULTS).fetch().get(0).value1();
    Map<String, Object> results = new LinkedHashMap<>();
    results.put("count", count);
    return results;
  }

  @RequestMapping("/ratings")
  public Map<String, Object> ratings() {
    Map<String, Object> results = new LinkedHashMap<>();
    results.putAll(ratingByColumn(RESULTS.IDE, RESULTS.IDE_RATING));
    results.putAll(ratingByColumn(RESULTS.LANGUAGE, RESULTS.LANGUAGE_RATING));
    results.putAll(ratingByColumn(RESULTS.APPSTACK, RESULTS.APPSTACK_RATING));
    results.putAll(ratingByColumn(RESULTS.ARCHITECTURE, RESULTS.ARCHITECTURE_RATING));
    results.putAll(ratingByColumn(RESULTS.DATABASE, RESULTS.DATABASE_RATING));
    return results;
  }

  @RequestMapping("/reasons")
  public Map<String, Object> reasons() {
    Map<String, Object> results = new LinkedHashMap<>();
    results.putAll(reasonByColumn(RESULTS.IDE, RESULTS.IDE_REASON, 10));
    results.putAll(reasonByColumn(RESULTS.LANGUAGE, RESULTS.LANGUAGE_REASON,10));
    results.putAll(reasonByColumn(RESULTS.APPSTACK, RESULTS.APPSTACK_REASON,10));
    results.putAll(reasonByColumn(RESULTS.ARCHITECTURE, RESULTS.ARCHITECTURE_REASON,10));
    results.putAll(reasonByColumn(RESULTS.DATABASE, RESULTS.DATABASE_REASON,10));
    results.putAll(reasonByColumn(RESULTS.DEVOPS, RESULTS.DEVOPS_REASON,10));
    results.putAll(reasonByColumn(RESULTS.PERFORMANCE, RESULTS.PERFORMANCE_REASON,10));
    return results;
  }

  @RequestMapping("/not-moving-reasons")
  public Map<String, Object> notMovingReasons() {
    Map<String, Object> results = new LinkedHashMap<>();
    results.put("IDE", reasonCount(RESULTS.IDE_MOVING_REASON));
    results.put("LANGUAGE", reasonCount(RESULTS.LANGUAGE_MOVING_REASON));
    results.put("APPSTACK", reasonCount(RESULTS.APPSTACK_MOVING_REASON));
    results.put("ARCHITECTURE", reasonCount(RESULTS.ARCHITECTURE_MOVING_REASON));
    results.put("DATABASE", reasonCount(RESULTS.DATABASE_MOVING_REASON));
    return results;
  }

  private Map<String, Double> reasonCount(TableField<Record, String> reasonColumn) {
    Result<Record2<String, Integer>> avg = dsl
      .select(reasonColumn, count().as("CNT"))
      .from(RESULTS)
      .where(reasonColumn.isNotNull())
      .groupBy(reasonColumn)
//      .having("CNT > " + lowThreshold)
      .orderBy(DSL.val(2).desc())
      .fetch();

    Map<String, Integer> props = new LinkedHashMap<>();
    avg.stream().forEach(r -> {
      if (r.value1() == null || r.value2() == null) { // skip "other" responses
        return;
      }
      props.put(r.value1(), r.value2());
    });
    return Misc.toPtc(props);
  }


  private Map<String, Object> reasonByColumn(TableField<Record, String> mainColumn, TableField<Record, String> reasonColumn, int lowThreshold) {
    Result<Record3<String, String, Integer>> avg = dsl
      .select(mainColumn, reasonColumn, count().as("CNT"))
      .from(RESULTS)
      .where(reasonColumn.isNotNull())
      .groupBy(mainColumn, reasonColumn).having("CNT > " + lowThreshold)//.orderBy(DSL.val("cnt").desc())
      .fetch();

    LinkedHashMap<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
    avg.stream().forEach(r -> {
      if (r.value1() == null || r.value2() == null) { // skip "other" responses
        return;
      }
      Map<String, Object> props = new HashMap<>();
      props.put("count", r.value3());
      props.put("reason", r.value2());
      if(result.containsKey(r.value1())) {
        result.get(r.value1()).add(props);
      }
      else {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(props);
        result.put(r.value1(), list);
      }
    });
    Misc.countListToPtc(result);
    Map<String, Object> x = new HashMap<>();
    x.put(reasonColumn.getName(), result);
    return x;
  }

  private Map<String, Object> ratingByColumn(TableField<Record, String> mainColumn, TableField<Record, BigDecimal> ratingColumn) {
    Result<Record3<String, BigDecimal, Integer>> avg = dsl
      .select(mainColumn, avg(ratingColumn).as("avg"), count())
      .from(RESULTS)
      .groupBy(mainColumn).orderBy(DSL.val(2).desc())
      .fetch();

    LinkedHashMap<String, Map<String, Object>> result = new LinkedHashMap<>();
    avg.stream().forEach(r -> {
      if (r.value1() == null) {
        return;
      }
      Map<String, Object> props = new HashMap<>();
      props.put("count", r.value3());
      props.put("rating", Misc.round(r.value2(), 1));
      result.put(r.value1(), props);
    });
    Misc.countToPtc(result);
    Map<String, Object> x = new HashMap<>();
    x.put(mainColumn.getName(), result);
    return x;
  }

}
