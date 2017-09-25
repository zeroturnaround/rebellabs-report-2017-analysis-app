package com.shelajev.rebellabs.report2017.analysisapp.controllers;

import com.shelajev.rebellabs.report2017.analysisapp.math.Misc;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.shelajev.rebellabs.report2017.analysisapp.model.tables.Results.RESULTS;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.count;

@RestController
@RequestMapping("/p/")
public class HappinessStats {

  private static final Logger log = LoggerFactory.getLogger(HappinessStats.class);

  private final DSLContext dsl;

  public HappinessStats(DSLContext dsl) {
    this.dsl = dsl;
  }

  @RequestMapping("/happiness-by-country")
  public Map<?, ?> happinessByCountry() {
    Result<Record3<String, BigDecimal, Integer>> records = dsl
      .select(RESULTS.COUNTRY,
        avg(RESULTS.IDE_RATING
          .add(RESULTS.LANGUAGE_RATING)
          .add(RESULTS.APPSTACK_RATING)
          .add(RESULTS.ARCHITECTURE_RATING)
          .add(RESULTS.DATABASE_RATING).div(5)), count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COUNTRY.isNotNull())
      .groupBy(RESULTS.COUNTRY).having("CNT > 2")
      .orderBy(DSL.val(2).desc())
      .fetch();
    return r2m(records, "average-rating", "count");
  }

  @RequestMapping("/happiness-by-job")
  public Map<?, ?> happinessByJob() {
    Result<Record3<String, BigDecimal, Integer>> records = dsl
      .select(RESULTS.JOB_DESCRIPTION,
        avg(RESULTS.IDE_RATING
          .add(RESULTS.LANGUAGE_RATING)
          .add(RESULTS.APPSTACK_RATING)
          .add(RESULTS.ARCHITECTURE_RATING)
          .add(RESULTS.DATABASE_RATING).div(5)), count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.JOB_DESCRIPTION.isNotNull())
      .groupBy(RESULTS.JOB_DESCRIPTION).having("CNT > 2")
      .orderBy(DSL.val(2).desc())
      .fetch();
    return r2m(records, "average-rating", "count");
  }

  @RequestMapping("/happiness-by-experience")
  public Map<?, ?> happinessByExperience() {
    Result<Record3<String, BigDecimal, Integer>> records = dsl
      .select(RESULTS.EXPERIENCE_YEARS,
        avg(RESULTS.IDE_RATING
          .add(RESULTS.LANGUAGE_RATING)
          .add(RESULTS.APPSTACK_RATING)
          .add(RESULTS.ARCHITECTURE_RATING)
          .add(RESULTS.DATABASE_RATING).div(5)), count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.EXPERIENCE_YEARS.isNotNull())
      .groupBy(RESULTS.EXPERIENCE_YEARS).having("CNT > 2")
      .orderBy(DSL.val(2).desc())
      .fetch();
    return r2m(records, "average-rating", "count");
  }

  @RequestMapping("/happiness-by-teamsize")
  public Map<?, ?> happinessByTeamSize() {
    Result<Record3<String, BigDecimal, Integer>> records = dsl
      .select(RESULTS.TEAMSIZE,
        avg(RESULTS.IDE_RATING
          .add(RESULTS.LANGUAGE_RATING)
          .add(RESULTS.APPSTACK_RATING)
          .add(RESULTS.ARCHITECTURE_RATING)
          .add(RESULTS.DATABASE_RATING).div(5)), count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.TEAMSIZE.isNotNull())
      .groupBy(RESULTS.TEAMSIZE).having("CNT > 2")
      .orderBy(DSL.val(2).desc())
      .fetch();
    return r2m(records, "average-rating", "count");
  }

  @RequestMapping("/happiness-by-company")
  public Map<?, ?> happinessByCompany() {
    Result<Record3<String, BigDecimal, Integer>> records = dsl
      .select(RESULTS.COMPANY,
        avg(RESULTS.IDE_RATING
          .add(RESULTS.LANGUAGE_RATING)
          .add(RESULTS.APPSTACK_RATING)
          .add(RESULTS.ARCHITECTURE_RATING)
          .add(RESULTS.DATABASE_RATING).div(5)), count().as("CNT"))
      .from(RESULTS)
      .where(RESULTS.COMPANY.isNotNull())
      .groupBy(RESULTS.COMPANY).having("CNT > 2")
      .orderBy(DSL.val(2).desc())
      .fetch();
    return r2m(records, "average-rating", "count");
  }


  private Map<?, ?> r2m(Result<Record3<String, BigDecimal, Integer>> records, String label2, String label3) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();

    records.stream().forEach(r -> {
      Map<String, Object> props = new HashMap<>();
      props.put(label2, Misc.round(r.value2(), 1));
      props.put(label3, r.value3());
      result.put(r.value1(), props);
    });
    log.info("result = {}", result);
    return result;
  }

}

