package org.springframework.data.mongodb.datatables;

import de.flapdoodle.embed.mongo.config.IMongoCmdOptions;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.io.IOException;

@Configuration
@EnableMongoRepositories(repositoryFactoryBeanClass = DataTablesRepositoryFactoryBean.class)
@EnableAutoConfiguration
public class TestConfiguration {

    /**
     * Version of the embedded MongoDB have to been set to >3.6.1 because of timezone support for aggregation function $dateToString
     *
     * @return
     * @throws IOException
     */
    @Bean
    public IMongodConfig prepareMongodConfig() throws IOException {
        IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder()
                .useNoJournal(false)
                .build();

        IMongodConfig mongoConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .configServer(false)
                .cmdOptions(cmdOptions)
                .build();
        return mongoConfig;
    }

}