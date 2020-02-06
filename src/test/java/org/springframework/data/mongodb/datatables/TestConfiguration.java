package org.springframework.data.mongodb.datatables;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(repositoryFactoryBeanClass = DataTablesRepositoryFactoryBean.class)
@EnableAutoConfiguration
public class TestConfiguration {

}