package com.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;
import java.util.Collections;
import java.util.Map;

@SpringBootApplication
@EnableBatchProcessing
public class BatchDemoJavaApplication {

	@Bean
	Log log(InjectionPoint ip) {
		return LogFactory.getLog(ip.getMember().getDeclaringClass());
	}

	public static class Person {

		private int age;
		private String firstName, email;

		public int getAge() {
			return age;
		}

		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public Person() {
		}

		@Override
		public String toString() {
			return "Person{" +
					"age=" + age +
					", firstName='" + firstName + '\'' +
					", email='" + email + '\'' +
					'}';
		}

		public Person(int age, String firstName, String email) {
			this.age = age;
			this.firstName = firstName;
			this.email = email;
		}
	}

	@Configuration
	public static class Step1Configuration {

		@Bean
		FlatFileItemReader<Person> csvReader(@Value("${input}") Resource resource) throws Exception {
			return new FlatFileItemReaderBuilder<Person>()
					.name("csv-to-person")
					.resource(resource)
					.targetType(Person.class)
					.delimited().delimiter(",").names("firstName,age,email".split(","))
					.build();
		}

		@Bean
		JdbcBatchItemWriter<Person> jdbcWriter(DataSource dataSource) {
			return new JdbcBatchItemWriterBuilder<Person>()
					.beanMapped()
					.dataSource(dataSource)
					.sql("insert into PEOPLE( FIRST_NAME, AGE, EMAIL) values ( :firstName, :age, :email)")
					.build();
		}
	}


	@Configuration
	public static class Step2Configuration {

		@Bean
		JdbcCursorItemReader<Map<Integer, Integer>> jdbcReader(DataSource dataSource) {
			return new JdbcCursorItemReaderBuilder<Map<Integer, Integer>>()
					.name("counts-to-map")
					.dataSource(dataSource)
					.sql("select COUNT(*) count, AGE age from PEOPLE group by AGE")
					.rowMapper((rs, i) -> Collections.singletonMap(
							rs.getInt("age"), rs.getInt("count")))
					.build();
		}

		@Bean
		FlatFileItemWriter<Map<Integer, Integer>> csvWriter(@Value("${output}") Resource output) {
			DelimitedLineAggregator<Map<Integer, Integer>> lineAggregator = new DelimitedLineAggregator<>();
			lineAggregator.setFieldExtractor(integerIntegerMap -> {
				Integer key = integerIntegerMap.keySet().iterator().next();
				return new Object[]{key, integerIntegerMap.get(key)};
			});
			return new FlatFileItemWriterBuilder<Map<Integer, Integer>>()
					.name("distribution-writer")
					.resource(output)
					.lineAggregator(lineAggregator)
					.build();
		}
	}


	@Bean
	Job job(Log log,
	        JobBuilderFactory jbf,
	        StepBuilderFactory sbf,
	        Step1Configuration step1,
	        Step2Configuration step2) throws Exception {

		Step s1 = sbf.get("csv-to-db")
				.<Person, Person>chunk(10)
				.reader(step1.csvReader(null))
				.writer(step1.jdbcWriter(null))
				.build();

		Step s2 = sbf.get("db-to-xml")
				.<Map<Integer, Integer>, Map<Integer, Integer>>chunk(10)
				.reader(step2.jdbcReader(null))
				.writer(step2.csvWriter(null))
				.build();

		return jbf.get("etl")
				.incrementer(new RunIdIncrementer())
				.start(s1)
				.next(s2)
				.build();
	}

	@Component
	public static class JobListener {

		private final Log log;

		public JobListener(Log log) {
			this.log = log;
		}

		@EventListener(JobExecutionEvent.class)
		public void onEvent(JobExecutionEvent executionEvent) {
			this.log.info(executionEvent.toString());
		}
	}


	public static void main(String[] args) {
		System.setProperty("input", "file://" +
				new File("/Users/jlong/Desktop/in.csv").getAbsolutePath());
		System.setProperty("output", "file://" +
				new File("/Users/jlong/Desktop/out.csv").getAbsolutePath());
		SpringApplication.run(BatchDemoJavaApplication.class, args);
	}
}
