package org.rrajesh1979.bitemp.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.rrajesh1979.bitemp.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import org.springframework.data.mongodb.core.aggregation.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Log4j2
public class BiTempService {
    public final MongoTemplate mongoTemplate;
    public MongoCollection<Document> mongoCollection;

    @Autowired
    public BiTempService(MongoTemplate mongoTemplate, MongoCollection<Document> mongoCollection) {
        this.mongoTemplate = mongoTemplate;
        this.mongoCollection = mongoCollection;
    }

    public String createBiTempData(CreateRequest createRequest) {
        log.debug("Create BiTemp Data: {}", createRequest);

        //Get related BiTempData
        List<BiTempObject> relatedBiTempData = getRelatedBiTempData(
                new GetRequest(
                        createRequest.id(),
                        createRequest.effectiveFrom(),
                        createRequest.effectiveTo()
                )
        );
        log.info("Related BiTemp Data: {}", relatedBiTempData);

        if (relatedBiTempData.size() > 0) {
            //Update related BiTempData
            //Two scenarios:
            //1. If the effectiveFrom and effectiveTo of new request overlaps with effectiveFrom and effectiveTo of existing BiTempData
            //2. If the effectiveFrom and effectiveTo of new request does not overlap with effectiveFrom and effectiveTo of existing BiTempData
            //In both the scenarios, we need to update the existing BiTempData
            //In the first scenario, we need to update the existing BiTempData with new effectiveTo
            //In the second scenario, we need to update the existing BiTempData with new effectiveFrom and effectiveTo
            //In both the scenarios, we need to insert a new BiTempData

            return "Update related BiTempData";
        } else {
            return insertBiTempData(createRequest);
        }
    }

    public String insertBiTempData(CreateRequest createRequest) {
        log.debug("Create BiTemp Data: {}", createRequest);

        BiTempObject biTempObject = convertRequestToBiTempObject(createRequest);

        Document biTempDocument = BiTempUtil.toBiTempObjectToDocument(biTempObject);
        InsertOneResult insertOneResult;
        try {
            insertOneResult = mongoCollection
                    .insertOne(biTempDocument);
            return Objects.requireNonNull(insertOneResult.getInsertedId()).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Document> getBiTempData(GetRequest getRequest) {
        log.debug("Get BiTemp Data: {}", getRequest);

        List<Document> pipeline = new ArrayList<>();
        Criteria matchKey = Criteria.where("key").is(getRequest.id());

        //getRequest.effectiveFrom() between "effectiveMeta.validFrom.dateTime" and "effectiveMeta.validTo.dateTime"
        Criteria matchEffectiveFrom = Criteria.where("effectiveMeta.validFrom.dateTime").lte(getRequest.effectiveFrom());
        Criteria matchEffectiveTo = Criteria.where("effectiveMeta.validTo.dateTime").gte(getRequest.effectiveTo());
        Criteria matchEffective = new Criteria().andOperator(matchEffectiveFrom, matchEffectiveTo);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "effectiveMeta.validFrom.dateTime");

        final Aggregation aggregation = Aggregation.newAggregation(matchStage, sortStage);

        AggregationResults<Document> aggregationResults =
                mongoTemplate.aggregate(aggregation, "forex", Document.class);

        return new ArrayList<>(aggregationResults.getMappedResults());
    }

    public List<BiTempObject> getRelatedBiTempData(GetRequest getRequest) {
        log.debug("Get Related BiTemp Data: {}", getRequest);

        List<Document> pipeline = new ArrayList<>();
        Criteria matchKey = Criteria.where("key").is(getRequest.id());

        //getRequest.effectiveFrom() between "effectiveMeta.validFrom.dateTime" and "effectiveMeta.validTo.dateTime"
        Criteria matchEffectiveFromS = Criteria.where("effectiveMeta.validFrom.dateTime").lte(getRequest.effectiveFrom());
        Criteria matchEffectiveFromE = Criteria.where("effectiveMeta.validTo.dateTime").gte(getRequest.effectiveFrom());
        Criteria matchEffectiveFrom = new Criteria().andOperator(matchEffectiveFromS, matchEffectiveFromE);

        Criteria matchEffectiveToS = Criteria.where("effectiveMeta.validFrom.dateTime").lte(getRequest.effectiveTo());
        Criteria matchEffectiveToE = Criteria.where("effectiveMeta.validTo.dateTime").gte(getRequest.effectiveTo());
        Criteria matchEffectiveTo = new Criteria().andOperator(matchEffectiveToS, matchEffectiveToE);

        Criteria matchEffective = new Criteria().orOperator(matchEffectiveFrom, matchEffectiveTo);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "effectiveMeta.validFrom.dateTime");

        final Aggregation aggregation = Aggregation.newAggregation(matchStage, sortStage);

        AggregationResults<BiTempObject> aggregationResults =
                mongoTemplate.aggregate(aggregation, "forex", BiTempObject.class);

        return new ArrayList<>(aggregationResults.getMappedResults());
    }

    private static BiTempObject convertRequestToBiTempObject(CreateRequest createRequest) {
        EffectiveMeta effectiveMeta = new EffectiveMeta(
                createRequest.effectiveFrom().atOffset(ZoneOffset.UTC),
                createRequest.effectiveTo().atOffset(ZoneOffset.UTC));
        RecordMeta recordMeta = new RecordMeta(
                createRequest.createdBy(),
                OffsetDateTime.now(),
                createRequest.createdBy(),
                OffsetDateTime.now());
        BiTempObject biTempObject = new BiTempObject(
                createRequest.id(),
                createRequest.data(),
                recordMeta,
                effectiveMeta,
                null,
                null);
        log.debug("BiTemp Object: {}", biTempObject);
        return biTempObject;
    }
}
