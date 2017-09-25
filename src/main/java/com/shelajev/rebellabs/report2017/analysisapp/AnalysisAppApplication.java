package com.shelajev.rebellabs.report2017.analysisapp;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.zeroturnaround.exec.ProcessExecutor;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
public class AnalysisAppApplication {

  private static final Logger log = LoggerFactory.getLogger(AnalysisAppApplication.class);

  // check localhost:8080/s/stats
  public static final Integer count = 2060;

  @Autowired
  DSLContext jooq;

  @Value(value = "classpath:sql/init.sql")
  private Resource initSQLfile;

  public static void main(String[] args) {
    SpringApplication.run(AnalysisAppApplication.class, args);
  }

  @Bean
  public CommandLineRunner prepareDatabase() {
    return (args) -> {
      jooq.execute("DROP ALL OBJECTS;");
      Stream<String> lines = Files.lines(Paths.get(initSQLfile.getURI()));
      lines.map(String::trim).filter(s -> !s.isEmpty()).peek(s -> log.info(s)).map(jooq::execute).count();
      log.info("jOOQ has tables: " + jooq.meta().getTables());
    };
  }

  @Bean
  public CommandLineRunner openWebApp() {
    return (args) -> {
      new ProcessExecutor().command("open", "http://localhost:8080").execute();
    };
  }
}
