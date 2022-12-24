package org.rrajesh1979.bitemp.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

@Configuration
@EnableMongoRepositories
public class MongoConfig {

    private final String mongoUri;
    private final String dbName;
    private final String collectionName;

    public MongoConfig(Environment env) {
        this.mongoUri = env.getProperty("spring.data.mongodb.uri");
        this.dbName = env.getProperty("spring.data.mongodb.database");
        this.collectionName = env.getProperty("spring.data.mongodb.collection");
    }

    @Bean
    public MongoClient mongoClient() {
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());

        var newCodecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec()),
                pojoCodecRegistry
        );

        final ConnectionString connectionString = new ConnectionString(mongoUri);
        final MongoClientSettings mongoClientSettings =
                MongoClientSettings.builder()
                        .codecRegistry(newCodecRegistry)
                        .applyConnectionString(connectionString).build();
        return MongoClients.create(mongoClientSettings);
    }

    @Bean
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(mongoClient(), dbName);
    }

    @Bean
    public MongoCollection<Document> getCollection() {

        return mongoTemplate()
                .getDb()
                .getCollection(collectionName);
    }
}
