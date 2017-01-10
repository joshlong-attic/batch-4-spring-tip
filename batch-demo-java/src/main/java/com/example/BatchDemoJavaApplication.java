package com.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;

@SpringBootApplication
@EnableBatchProcessing
public class BatchDemoJavaApplication {

	public static class Person {
		private int age;
		private String name, email;

		public int getAge() {
			return age;
		}

		@Override
		public String toString() {
			return "Person{" +
					"age=" + age +
					", name='" + name + '\'' +
					", email='" + email + '\'' +
					'}';
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public Person() {
		}

		public Person(int age, String name, String email) {
			this.age = age;
			this.name = name;
			this.email = email;
		}
	}

	@Bean
	FlatFileItemReader<Person> fileReader(@Value("${dailyFile}") Resource resource) throws Exception {
		return new FlatFileItemReaderBuilder<Person>()
				.name("csv-to-person")
				.resource(resource)
				.targetType(Person.class)
				.delimited().delimiter(",").names("name,age,email".split(","))
				.build();
	}

	@Bean
	JdbcBatchItemWriter<Person> jdbcWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
				.beanMapped()
				.dataSource(dataSource)
				.sql("insert into PEOPLE( NAME, AGE, EMAIL) values ( :name, :age, :email)")
				.build();
	}

	@Bean
	Job job(JobBuilderFactory jbf,
	        StepBuilderFactory sbf,
	        ItemReader<Person> ir,
	        ItemWriter<Person> iw) {

		Step step1 = sbf.get("csv-to-db")
				.<Person, Person>chunk(10)
				.reader(ir)
				.writer(iw)
				.build();

		return jbf.get("etl")
				.flow(step1)
				.end()
				.build();
	}

	@Component
	public static class JobListener {

		private final JdbcTemplate jdbcTemplate;

		public JobListener(JdbcTemplate jdbcTemplate) {
			this.jdbcTemplate = jdbcTemplate;
		}

		@EventListener(JobExecutionEvent.class)
		public void onEvent(JobExecutionEvent executionEvent) {
			this.jdbcTemplate
					.query("select * from PEOPLE", (res, i) -> new Person(
							res.getInt("AGE"),
							res.getString("NAME"),
							res.getString("EMAIL")))
					.forEach(System.out::println);
		}
	}


	public static void main(String[] args) {
		System.setProperty("dailyFile", "file://" +
				new File("/Users/jlong/Desktop/in.csv").getAbsolutePath());
		SpringApplication.run(BatchDemoJavaApplication.class, args);
	}
}
