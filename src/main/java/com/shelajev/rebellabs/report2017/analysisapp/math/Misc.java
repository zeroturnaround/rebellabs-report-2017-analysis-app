package com.shelajev.rebellabs.report2017.analysisapp.math;

import com.shelajev.rebellabs.report2017.analysisapp.AnalysisAppApplication;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Misc {

  public static final int PTC_SCALE = 3;

  public static double round(BigDecimal value, int places) {
    if (places < 0) throw new IllegalArgumentException();

    value = value.setScale(places, RoundingMode.HALF_UP);
    return value.doubleValue();
  }

  public static double ptc(Integer value, Integer count) {
    return ptc(BigDecimal.valueOf(value), count);
  }

  public static double ptc(BigDecimal value) {
    return ptc(value, AnalysisAppApplication.count);
  }
  private static double ptc (BigDecimal value, Integer count){
    return value.divide(BigDecimal.valueOf(count), PTC_SCALE, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100)).doubleValue();
  }

  public static double ptc(int value) {
    return ptc(BigDecimal.valueOf(value));
  }

  public static double ptc(double value) {
    return ptc(BigDecimal.valueOf(value));
  }

  public static Map<String, Double> toPtc(Map<String, Integer> map) {
    Integer sum = map.values().stream().collect(Collectors.summingInt(x -> x));
    Map<String, Double> results = new LinkedHashMap<>();
    map.keySet().stream().forEach(k -> {
      Integer v = map.get(k);
      results.put(k, ptc(v, sum));
    });
    return results;
  }

  public static LinkedHashMap<String,Map<String,Object>> countToPtc(LinkedHashMap<String, Map<String, Object>> map) {
    Integer sum = map.values().stream().collect(Collectors.summingInt(x -> (Integer) x.get("count")));
    map.keySet().stream().forEach(k -> {
      Map<String, Object> innerMap = map.get(k);
      Integer count = (Integer) innerMap.get("count");
      innerMap.put("ptc", ptc(count, sum));
    });
    return map;
  }

  public static void countListToPtc(LinkedHashMap<String, List<Map<String, Object>>> result) {
    Map<String, Integer> sums = new HashMap<>();
    result.keySet().stream().forEach(k -> {
      List<Map<String, Object>> reasons = result.get(k);
      reasons.forEach(m -> {
        Integer count = (Integer) m.get("count");
        if(sums.containsKey(k)) {
          sums.put(k, sums.get(k) + count);
        }
        else {
          sums.put(k, count);
        }
      });
    });
    result.keySet().stream().forEach(k -> {
      List<Map<String, Object>> reasons = result.get(k);
      reasons.forEach(m -> {
        Integer count = (Integer) m.get("count");
        m.put("ptc", ptc(count, sums.get(k)));
      });
    });
  }
}
