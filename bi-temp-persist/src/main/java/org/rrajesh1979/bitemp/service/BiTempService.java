package org.rrajesh1979.bitemp.service;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.rrajesh1979.bitemp.config.OffsetDateTimeCodec;
import org.rrajesh1979.bitemp.model.BiTempUtil;
import org.rrajesh1979.bitemp.model.BiTempObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@Log4j2
public class BiTempService {
    public final MongoTemplate mongoTemplate;
    public MongoCollection<Document> mongoCollection;

    @Autowired
    public BiTempService(MongoTemplate mongoTemplate) {
        //Mongo Codec Registry
        var newCodecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(new OffsetDateTimeCodec())
        );

        this.mongoTemplate = mongoTemplate;

        String collectionName = "forex"; //TODO: Make this configurable
        this.mongoCollection =
                this.mongoTemplate
                        .getDb()
                        .getCollection(collectionName)
                        .withCodecRegistry(newCodecRegistry);
    }

    public String createBiTempData(BiTempObject biTempObject) {
        log.info("Create BiTemp Data: {}", biTempObject);
        Document biTempDocument = BiTempUtil.toBiTempObjectToDocument(biTempObject);
        InsertOneResult insertOneResult = null;
        try {
            insertOneResult = mongoCollection
                    .insertOne(biTempDocument);
            return insertOneResult.getInsertedId().toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Document> getBiTempData(Object key, LocalDateTime effectiveFrom, LocalDateTime effectiveTo) {
        log.info("Get BiTemp Data: {}", key);
        Query query = new Query();
        query.addCriteria(Criteria.where("key").is(key));
        query.addCriteria(Criteria.where("effectiveMeta.validFrom.dateTime").gte(effectiveFrom));
        query.addCriteria(Criteria.where("effectiveMeta.validTo.dateTime").lte(effectiveTo));

        log.info("Query: {}", query);

        List<Document> result = new ArrayList<>();

        mongoCollection.find(query.getQueryObject())
                .iterator()
                .forEachRemaining(result::add);

        return result;
    }
}
