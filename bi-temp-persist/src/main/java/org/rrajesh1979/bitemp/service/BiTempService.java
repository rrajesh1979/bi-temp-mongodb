package org.rrajesh1979.bitemp.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.rrajesh1979.bitemp.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import org.springframework.data.mongodb.core.aggregation.*;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Log4j2
public class BiTempService {
    public final MongoTemplate mongoTemplate;
    public MongoCollection<Document> mongoCollection;

    //EST
    public static final ZoneOffset zoneOffSet= ZoneOffset.of("-05:00");

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
                        createRequest.key(),
                        createRequest.effectiveFrom(),
                        createRequest.effectiveTo()
                )
        );
        log.info("Related BiTemp Data: {}", relatedBiTempData);

        if (relatedBiTempData.size() == 1) {
            //Scenario 1: One Matching Record
            LocalDateTime newFrom = createRequest.effectiveFrom();
            LocalDateTime newTo = createRequest.effectiveTo();

            OffsetDateTime existingFromDateTime = relatedBiTempData.get(0).effectiveMeta().validFrom();
            String existingFromOffset = relatedBiTempData.get(0).effectiveMeta().validFrom().getOffset().toString();
            LocalDateTime existingFrom = LocalDateTime.ofInstant(existingFromDateTime.toInstant(), ZoneOffset.of(existingFromOffset));

            OffsetDateTime existingToDateTime = relatedBiTempData.get(0).effectiveMeta().validTo();
            String existingToOffset = relatedBiTempData.get(0).effectiveMeta().validTo().getOffset().toString();
            LocalDateTime existingTo = LocalDateTime.ofInstant(existingToDateTime.toInstant(), ZoneOffset.of(existingToOffset));

            Object existingData = relatedBiTempData.get(0).data();

            ObjectId existingId = relatedBiTempData.get(0)._id();

            if (newFrom.isEqual(existingFrom) && newTo.isEqual(existingTo)) {
                // Scenario 1.1: Exact Match. [NewFrom, NewTo] = [ExistingFrom, ExistingTo]
                // Ex: New[2022-12-25, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Update the existing record
                log.info("Scenario 1.1: Exact Match. [NewFrom, NewTo] = [ExistingFrom, ExistingTo]");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("data", createRequest.data()))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);
                return updateResult.toString();

            } else if (newFrom.isAfter(existingFrom) && newTo.isEqual(existingTo) ) {
                // Scenario 1.2: New From is after existing From and New To is equal to existing To
                // Ex: New[2022-12-26, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second] and New[2022-12-26, 2022-12-31]
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record
                log.info("Scenario 1.2: New From is after existing From and New To is equal to existing To");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveTo", newFrom.minusSeconds(1)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                return updateResult + " & " + insertResult;

            } else if (newFrom.isBefore(existingFrom) && newTo.isEqual(existingTo)) {
                //Scenario 1.3: New From is before existing From and New To is equal to existing To
                // Ex: New[2022-12-24, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-31] and New[2022-12-24, 2022-12-25 - 1 second]
                // Update the new record's validTo to existing record's effectiveFrom - 1 second and insert new record

                log.info("Scenario 1.3: New From is before existing From and New To is equal to existing To");
                CreateRequest newCreateRequest = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        createRequest.effectiveFrom(),
                        existingTo.minusSeconds(1)
                );

                return insertBiTempData(newCreateRequest);

            } else if (newFrom.isEqual(existingFrom) && newTo.isBefore(existingTo)) {
                //Scenario 1.4: New From is equal to existing From and New To is before existing To
                // Ex: New[2022-12-25, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-30 + 1 second, 2022-12-31] and New[2022-12-25, 2022-12-30]
                // Update the existing record's validFrom to new record's effectiveTo + 1 second and insert new record

                log.info("Scenario 1.4: New From is equal to existing From and New To is before existing To");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveFrom", newTo.plusSeconds(1)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                return updateResult + " & " + insertResult;

            } else if (newFrom.isEqual(existingFrom) && newTo.isAfter(existingTo)) {
                //Scenario 1.5: New From is equal to existing From and New To is after existing To
                // Ex: New[2022-12-25, 2023-01-01] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: Existing[2022-12-25, 2022-12-31] D2 and New[2022-12-31 + 1 second, 2023-01-01] D1
                // Update the new record's validFrom to existing record's effectiveTo + 1 second and insert new record

                log.info("Scenario 1.5: New From is equal to existing From and New To is after existing To");
                CreateRequest newCreateRequest = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        existingTo.plusSeconds(1),
                        createRequest.effectiveTo()
                );

                return insertBiTempData(newCreateRequest);

            } else if (newFrom.isAfter(existingFrom) && newTo.isBefore(existingTo)) {
                //Scenario 1.6: New From is after existing From and New To is before existing To
                // Ex: New[2022-12-26, 2022-12-30] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: ExistingSplitA[2022-12-25, 2022-12-26 - 1 second] D2, New[2022-12-26, 2022-12-30] D1, ExistingSplitB[2022-12-30 + 1 second, 2022-12-31] D2
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record
                // Insert new record with validFrom = new record's effectiveTo + 1 second and validTo = existing record's effectiveTo

                log.info("Scenario 1.6: New From is after existing From and New To is before existing To");
                UpdateResult updateResultA = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveTo", newFrom.minusSeconds(1)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result A: {}", updateResultA);

                String insertResult = insertBiTempData(createRequest);

                CreateRequest newCreateRequest = new CreateRequest(
                        createRequest.key(),
                        existingData,
                        createRequest.createdBy(),
                        newTo.plusSeconds(1),
                        existingTo
                );

                return updateResultA + " & " + insertResult + " & " + insertBiTempData(newCreateRequest);

            } else if (newFrom.isBefore(existingFrom) && newTo.isAfter(existingTo)) {
                //Scenario 1.7: New From is before existing From and New To is after existing To
                // Ex: New[2022-12-24, 2023-01-01] = Existing[2022-12-25, 2022-12-31]
                // Result: NewSplitA[2022-12-24, 2022-12-25 - 1 second], Existing[2022-12-25, 2022-12-31], NewSplitB[2022-12-31 + 1 second, 2023-01-01]
                // Update the new record's validTo to existing record's effectiveFrom - 1 second and insert new record
                // Insert new record with validFrom = existing record's effectiveTo + 1 second and validTo = new record's effectiveTo

                log.info("Scenario 1.7: New From is before existing From and New To is after existing To");
                CreateRequest newCreateRequestA = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        createRequest.effectiveFrom(),
                        existingFrom.minusSeconds(1)
                );

                String insertResultA = insertBiTempData(newCreateRequestA);

                CreateRequest newCreateRequestB = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        existingTo.plusSeconds(1),
                        createRequest.effectiveTo()
                );

                String insertResultB = insertBiTempData(newCreateRequestB);

                return insertResultA + " & " + insertResultB;

            } else if (newFrom.isAfter(existingFrom) && newTo.isAfter(existingTo)) {
                //Scenario 1.8: New From is after existing From and New To is after existing To
                // Ex: New[2022-12-26, 2023-01-01] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second], New[2022-12-26, 2023-01-01]
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record

                log.info("Scenario 1.8: New From is after existing From and New To is after existing To");

                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveTo", newFrom.minusSeconds(1)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                return updateResult + " & " + insertResult;

            } else if (newFrom.isBefore(existingFrom) && newTo.isBefore(existingTo)) {
                //Scenario 1.9: New From is before existing From and New To is before existing To
                // Ex: New[2022-12-24, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: New[2022-12-24, 2022-12-30], Existing[2022-12-30 + 1 second, 2022-12-31]
                // Update the new record's validTo to existing record's effectiveFrom - 1 second and insert new record

                log.info("Scenario 1.9: New From is before existing From and New To is before existing To");

                String insertResult = insertBiTempData(createRequest);

                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveFrom", newTo.plusSeconds(1)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );

                log.info("Update Result: {}", updateResult);

                return insertResult + " & " + updateResult;
            }

            return "Did not match any scenario";

        } else if (relatedBiTempData.size() == 2) {
            //Scenario 2: Two Matching Records
            return "Update related BiTempData";
        } else if (relatedBiTempData.size() > 2) {
            //Scenario 2: Two Matching Records
            return "Invalid scenario";
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

        EffectiveMeta effectiveMetaQuery = new EffectiveMeta(
                getRequest.effectiveFrom().atOffset(zoneOffSet),
                getRequest.effectiveTo().atOffset(zoneOffSet));

        Long newFrom = effectiveMetaQuery.validFrom().toInstant().toEpochMilli();
        Long newTo = effectiveMetaQuery.validTo().toInstant().toEpochMilli();

        List<Document> pipeline = new ArrayList<>();
        Criteria matchKey = Criteria.where("key").is(getRequest.id());

        /*Criteria matchEffectiveFromS = Criteria.where("effectiveMeta.validFrom.dateTime").lte(effectiveMetaQuery.validFrom());
        Criteria matchEffectiveFromE = Criteria.where("effectiveMeta.validTo.dateTime").gte(effectiveMetaQuery.validFrom());
        Criteria matchEffectiveFrom = new Criteria().andOperator(matchEffectiveFromS, matchEffectiveFromE);

        Criteria matchEffectiveToS = Criteria.where("effectiveMeta.validFrom.dateTime").lte(effectiveMetaQuery.validTo());
        Criteria matchEffectiveToE = Criteria.where("effectiveMeta.validTo.dateTime").gte(effectiveMetaQuery.validTo());
        Criteria matchEffectiveTo = new Criteria().andOperator(matchEffectiveToS, matchEffectiveToE);*/

        Criteria matchEffectiveFromS = Criteria.where("effectiveMeta.validFrom.ephochMilli").lte(newFrom);
        Criteria matchEffectiveFromE = Criteria.where("effectiveMeta.validTo.ephochMilli").gte(newFrom);
        Criteria matchEffectiveFrom = new Criteria().andOperator(matchEffectiveFromS, matchEffectiveFromE);

        Criteria matchEffectiveToS = Criteria.where("effectiveMeta.validFrom.ephochMilli").lte(newTo);
        Criteria matchEffectiveToE = Criteria.where("effectiveMeta.validTo.ephochMilli").gte(newTo);
        Criteria matchEffectiveTo = new Criteria().andOperator(matchEffectiveToS, matchEffectiveToE);

        Criteria matchEffective = new Criteria().orOperator(matchEffectiveFrom, matchEffectiveTo);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "effectiveMeta.validFrom.ephochMilli");

        final Aggregation aggregation = Aggregation.newAggregation(matchStage, sortStage);

        AggregationResults<BiTempObject> aggregationResults =
                mongoTemplate.aggregate(aggregation, "forex", BiTempObject.class);

        return new ArrayList<>(aggregationResults.getMappedResults());
    }

    private static BiTempObject convertRequestToBiTempObject(CreateRequest createRequest) {
        EffectiveMeta effectiveMeta = new EffectiveMeta(
                createRequest.effectiveFrom().atOffset(zoneOffSet),
                createRequest.effectiveTo().atOffset(zoneOffSet));
        RecordMeta recordMeta = new RecordMeta(
                createRequest.createdBy(),
                OffsetDateTime.now(),
                createRequest.createdBy(),
                OffsetDateTime.now());
        BiTempObject biTempObject = new BiTempObject(
                createRequest.key(),
                createRequest.data(),
                recordMeta,
                effectiveMeta,
                null,
                null,
                null);
        log.debug("BiTemp Object: {}", biTempObject);
        return biTempObject;
    }
}
