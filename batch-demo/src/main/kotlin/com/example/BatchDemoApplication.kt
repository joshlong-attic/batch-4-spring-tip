package com.example

import org.springframework.batch.core.Job
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.event.EventListener
import org.springframework.core.io.Resource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.io.File
import javax.sql.DataSource

@SpringBootApplication
@EnableBatchProcessing
open class BatchDemoApplication {

    data class Person(var name: String? = null, var age: Int? = 0, var email: String? = null)

    @Bean
    open fun fileReader(@Value("\${dailyFile}") r: Resource): FlatFileItemReader<Person> =
            FlatFileItemReaderBuilder <Person>()
                    .name("csv-to-person")
                    .resource(r)
                    .targetType(Person::class.java)
                    .delimited().delimiter(",").names(arrayOf("name", "age", "email"))
                    .build()


    @Bean
    open fun jdbcWriter(ds: DataSource): JdbcBatchItemWriter<Person> =
            JdbcBatchItemWriterBuilder<Person>()
                    .dataSource(ds)
                    .beanMapped()
                    .sql("insert into PEOPLE (NAME, AGE, EMAIL) values (:name, :age, :email)")
                    .build()

    @Bean
    open fun job(jbf: JobBuilderFactory, sbf: StepBuilderFactory,
                 ir: ItemReader<Person>,
                 iw: ItemWriter<Person>): Job {

        val step1 = sbf.get("csv-to-db")
                .chunk<Person, Person>(10)
                .reader(ir)
                .writer(iw)
                .build()

        return jbf.get("etl")
                .flow(step1)
                .end()
                .build()
    }

    @Component
    open class JobListener(val jdbcTemplate: JdbcTemplate) {

        @EventListener(JobExecutionEvent::class)
        open fun jobEvent(jee: JobExecutionEvent) {
            println("job execution event: ${jee}")
            this.jdbcTemplate.query("select * from PEOPLE", { rs, i ->
                Person(rs.getString("NAME"), rs.getInt("AGE"), rs.getString("EMAIL"))
            }).forEach(::println)
        }
    }
}

fun main(args: Array<String>) {
    val absolutePath = File("/Users/jlong/Desktop/in.csv").absolutePath
    System.setProperty("dailyFile", "file://${absolutePath}")
    SpringApplication.run(BatchDemoApplication::class.java, *args)
}
