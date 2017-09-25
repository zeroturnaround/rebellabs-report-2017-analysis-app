package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

import static com.shelajev.rebellabs.report2017.analysisapp.model.tables.Results.RESULTS;
import static org.jooq.impl.DSL.count;

@RestController
@RequestMapping("/p/")
public class PivotStats {

  private static final Logger log = LoggerFactory.getLogger(PivotStats.class);

  private final DSLContext dsl;

  public PivotStats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/devops-by-arch")
  public Map<?, ?> devopsByArch() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.ARCHITECTURE, RESULTS.DEVOPS, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.ARCHITECTURE.isNotNull()).and(RESULTS.DEVOPS.isNotNull())
      .groupBy(RESULTS.ARCHITECTURE, RESULTS.DEVOPS).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "devops", "count");
  }

  @RequestMapping("/devops-by-appstack")
  public Map<?, ?> devopsByAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.APPSTACK, RESULTS.DEVOPS, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.APPSTACK.isNotNull()).and(RESULTS.DEVOPS.isNotNull())
      .groupBy(RESULTS.APPSTACK, RESULTS.DEVOPS).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "devops", "count");
  }

  @RequestMapping("/devops-reasons-by-arch")
  public Map<?, ?> devopsReasonsByArch() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.ARCHITECTURE, RESULTS.DEVOPS_REASON, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.ARCHITECTURE.isNotNull()).and(RESULTS.DEVOPS_REASON.isNotNull())
      .groupBy(RESULTS.ARCHITECTURE, RESULTS.DEVOPS_REASON).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "devops-reason", "count");
  }

  @RequestMapping("/devops-reasons-by-appstack")
  public Map<?, ?> devopsReasonsByAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.APPSTACK, RESULTS.DEVOPS_REASON, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.APPSTACK.isNotNull()).and(RESULTS.DEVOPS_REASON.isNotNull())
      .groupBy(RESULTS.APPSTACK, RESULTS.DEVOPS_REASON).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "devops-reason", "count");
  }

  @RequestMapping("/performance-by-arch")
  public Map<?, ?> performanceByArch() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.ARCHITECTURE, RESULTS.PERFORMANCE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.ARCHITECTURE.isNotNull()).and(RESULTS.PERFORMANCE.isNotNull())
      .groupBy(RESULTS.ARCHITECTURE, RESULTS.PERFORMANCE).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "performance", "count");
  }

  @RequestMapping("/performance-by-appstack")
  public Map<?, ?> performanceByAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.APPSTACK, RESULTS.PERFORMANCE, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.APPSTACK.isNotNull()).and(RESULTS.PERFORMANCE.isNotNull())
      .groupBy(RESULTS.APPSTACK, RESULTS.PERFORMANCE).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "performance", "count");
  }

  @RequestMapping("/performance-reasons-by-arch")
  public Map<?, ?> performanceReasonsByArch() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.ARCHITECTURE, RESULTS.PERFORMANCE_REASON, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.ARCHITECTURE.isNotNull()).and(RESULTS.PERFORMANCE_REASON.isNotNull())
      .groupBy(RESULTS.ARCHITECTURE, RESULTS.PERFORMANCE_REASON).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "performance-reason", "count");
  }

  @RequestMapping("/performance-reasons-by-appstack")
  public Map<?, ?> performanceReasonsByAppstack() {
    Result<Record3<String, String, Integer>> records = dsl
      .select(RESULTS.APPSTACK, RESULTS.PERFORMANCE_REASON, count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.APPSTACK.isNotNull()).and(RESULTS.PERFORMANCE_REASON.isNotNull())
      .groupBy(RESULTS.APPSTACK, RESULTS.PERFORMANCE_REASON).having("CNT > 2")
      .orderBy(DSL.val(3).desc())
      .fetch();
    return r2m(records, "performance-reason", "count");
  }

  private Map<?, ?> r2m(Result<Record3<String, String, Integer>> records, String label2, String label3) {
    LinkedHashMap<String, List<Map<String, Object>>> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put(label2, r.value2());
      props.put(label3, r.value3());
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

