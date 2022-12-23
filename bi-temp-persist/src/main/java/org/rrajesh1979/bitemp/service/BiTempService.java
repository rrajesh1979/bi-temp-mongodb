package org.rrajesh1979.bitemp.service;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.rrajesh1979.bitemp.config.OffsetDateTimeCodec;
import org.rrajesh1979.bitemp.model.BiTempDocument;
import org.rrajesh1979.bitemp.model.BiTempObject;
import org.rrajesh1979.bitemp.resource.BiTempResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class BiTempService {
    public final MongoTemplate mongoTemplate;
    public MongoCollection<Document> mongoCollection;

    @Value("${spring.data.mongodb.collection}")
    private String collectionName;

    @Autowired
    public BiTempService(MongoTemplate mongoTemplate) {
        //Mongo Codec Registry
        var newCodecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec())
        );

        this.mongoTemplate = mongoTemplate;

        log.info("Collection Name: {}", collectionName);
        this.mongoCollection =
                this.mongoTemplate
                        .getDb()
                        .getCollection("forex")
                        .withCodecRegistry(newCodecRegistry);
    }

    public String createBiTempData(BiTempObject biTempObject) {
        log.info("Create BiTemp Data: {}", biTempObject);
        log.info("Collection Name: {}", collectionName);
        BiTempDocument biTempDocument = new BiTempDocument(biTempObject);
        InsertOneResult insertOneResult = null;
        try {
            log.info("OffsetDateTimeCodec Registry: {}", mongoCollection.getCodecRegistry().toString());
            insertOneResult = mongoCollection
                    .insertOne(biTempDocument.toDocument());
            return insertOneResult.getInsertedId().toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
