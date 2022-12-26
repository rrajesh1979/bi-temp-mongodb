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
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import org.springframework.data.mongodb.core.aggregation.*;

import java.time.*;
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
            long newFrom = createRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
            long newTo = createRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

            long existingFrom = relatedBiTempData.get(0).effectiveMeta().validFrom().toInstant().toEpochMilli();
            long existingTo = relatedBiTempData.get(0).effectiveMeta().validTo().toInstant().toEpochMilli();

            Object existingData = relatedBiTempData.get(0).data();
            ObjectId existingId = relatedBiTempData.get(0)._id();

            if (newFrom == existingFrom && newTo == existingTo) {
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
                CreateResponse createResponse = new CreateResponse(
                        List.of(),
                        List.of(updateResult.toString())
                );
                return createResponse.toString();

            } else if (newFrom > existingFrom && newTo == existingTo ) {
                // Scenario 1.2: New From is after existing From and New To is equal to existing To
                // Ex: New[2022-12-26, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second] and New[2022-12-26, 2022-12-31]
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record
                log.info("Scenario 1.2: New From is after existing From and New To is equal to existing To");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult.toString())
                );

                return createResponse.toString();

            } else if (newFrom < existingFrom && newTo == existingTo) {
                //Scenario 1.3: New From is before existing From and New To is equal to existing To
                // Ex: New[2022-12-24, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-24, 2022-12-31]
                // Update the existing record's validFrom to new record's effectiveFrom.
                // Update the existing record's data. No need to insert new record

                log.info("Scenario 1.3: New From is before existing From and New To is equal to existing To");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set",
                                new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(newFrom, existingTo))
                                .append("data", createRequest.data()))
                );

                log.info("Update Result: {}", updateResult);

                CreateResponse createResponse = new CreateResponse(
                        List.of(),
                        List.of(updateResult.toString())
                );

                return createResponse.toString();

            } else if (newFrom == existingFrom && newTo < existingTo) {
                //Scenario 1.4: New From is equal to existing From and New To is before existing To
                // Ex: New[2022-12-25, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-30 + 1 second, 2022-12-31] and New[2022-12-25, 2022-12-30]
                // Update the existing record's validFrom to new record's effectiveTo + 1 second and insert new record

                log.info("Scenario 1.4: New From is equal to existing From and New To is before existing To");
                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set",
                                new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(newTo + 1000, existingTo))
                        )
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult.toString())
                );

                return createResponse.toString();

            } else if (newFrom == existingFrom && newTo > existingTo) {
                //Scenario 1.5: New From is equal to existing From and New To is after existing To
                // Ex: New[2022-12-25, 2023-01-01] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: Existing[2022-12-25, 2022-12-31] D2 and New[2022-12-31 + 1 second, 2023-01-01] D1
                // Update the new record's validFrom to existing record's effectiveTo + 1 second and insert new record

                log.info("Scenario 1.5: New From is equal to existing From and New To is after existing To");
                CreateRequest newCreateRequest = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(existingTo + 1000), zoneOffSet),
                        createRequest.effectiveTo()
                );

                String insertResult = insertBiTempData(newCreateRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of()
                );

                return createResponse.toString();

            } else if (newFrom > existingFrom && newTo < existingTo) {
                //Scenario 1.6: New From is after existing From and New To is before existing To
                // Ex: New[2022-12-26, 2022-12-30] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: ExistingSplitA[2022-12-25, 2022-12-26 - 1 second] D2, New[2022-12-26, 2022-12-30] D1, ExistingSplitB[2022-12-30 + 1 second, 2022-12-31] D2
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record
                // Insert new record with validFrom = new record's effectiveTo + 1 second and validTo = existing record's effectiveTo

                log.info("Scenario 1.6: New From is after existing From and New To is before existing To");
                UpdateResult updateResultA = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result A: {}", updateResultA);

                String insertResult = insertBiTempData(createRequest);

                CreateRequest newCreateRequest = new CreateRequest(
                        createRequest.key(),
                        existingData,
                        createRequest.createdBy(),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(newTo + 1000), zoneOffSet),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(existingTo), zoneOffSet)
                );

                String insertResultB = insertBiTempData(newCreateRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult, insertResultB),
                        List.of(updateResultA.toString())
                );

                return createResponse.toString();

            } else if (newFrom < existingFrom && newTo > existingTo) {
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
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(existingFrom - 1000), zoneOffSet)
                );

                String insertResultA = insertBiTempData(newCreateRequestA);

                CreateRequest newCreateRequestB = new CreateRequest(
                        createRequest.key(),
                        createRequest.data(),
                        createRequest.createdBy(),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(existingTo + 1000), zoneOffSet),
                        createRequest.effectiveTo()
                );

                String insertResultB = insertBiTempData(newCreateRequestB);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResultA, insertResultB),
                        List.of()
                );

            } else if (newFrom > existingFrom && newTo > existingTo) {
                //Scenario 1.8: New From is after existing From and New To is after existing To
                // Ex: New[2022-12-26, 2023-01-01] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second], New[2022-12-26, 2023-01-01]
                // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record

                log.info("Scenario 1.8: New From is after existing From and New To is after existing To");

                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );
                log.info("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult.toString())
                );

                return createResponse.toString();

            } else if (newFrom < existingFrom && newTo < existingTo) {
                //Scenario 1.9: New From is before existing From and New To is before existing To
                // Ex: New[2022-12-24, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: New[2022-12-24, 2022-12-30], Existing[2022-12-30 + 1 second, 2022-12-31]
                // Update the new record's validTo to existing record's effectiveFrom - 1 second and insert new record

                log.info("Scenario 1.9: New From is before existing From and New To is before existing To");

                String insertResult = insertBiTempData(createRequest);

                UpdateResult updateResult = mongoCollection.updateOne(
                        new Document("_id", existingId),
                        new Document("$set", new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(newTo + 1000, existingTo)))
                        //TODO: Update UpdatedBy and UpdatedOn
                );

                log.info("Update Result: {}", updateResult);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult.toString())
                );

                return createResponse.toString();
            }

            return "Did not match any scenario";

        } else if (relatedBiTempData.size() == 2) {
            //Scenario 2: Two Matching Records
            // Ex: New[2022-12-26, 2022-01-01] = Existing[2022-12-25, 2022-12-30] & Existing[2022-12-31, 2023-01-05]
            // Result: Existing[2022-12-25, 2022-12-26 - 1 second], New[2022-12-26, 2023-01-01], Existing[2023-01-01 + 1 second, 2023-01-05]
            // Update the existing record's validTo to new record's effectiveFrom - 1 second and insert new record
            // Insert new record with validFrom = new record's effectiveTo + 1 second and validTo = existing record's effectiveTo

            log.info("Scenario 2: Two Matching Records");

            long newFrom = createRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
            long newTo = createRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

            long existingFromA = relatedBiTempData.get(0).effectiveMeta().validFrom().toInstant().toEpochMilli();
            long existingToA = newFrom - 1000;
            ObjectId existingIdA = relatedBiTempData.get(0)._id();

            long existingFromB = newTo + 1000;
            long existingToB = relatedBiTempData.get(1).effectiveMeta().validTo().toInstant().toEpochMilli();
            ObjectId existingIdB = relatedBiTempData.get(1)._id();

            UpdateResult updateResultA = mongoCollection.updateOne(
                    new Document("_id", existingIdA),
                    new Document("$set",
                            new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(existingFromA, existingToA))
                    ));

            String insertResult = insertBiTempData(createRequest);

            UpdateResult updateResultB = mongoCollection.updateOne(
                    new Document("_id", existingIdB),
                    new Document("$set",
                            new Document("effectiveMeta", convertEpochMilliToEffectiveMeta(existingFromB, existingToB))
                    ));

            CreateResponse createResponse = new CreateResponse(
                    List.of(insertResult),
                    List.of(updateResultA.toString(), updateResultB.toString())
            );

            return createResponse.toString();

        } else if (relatedBiTempData.size() > 2) {
            //Scenario Invalid: More than Two Matching Records. Avoid getting into this scenario
            return "Invalid scenario";
        } else {
            //Scenario 3: No Matching Records
            // Ex: New[2022-12-26, 2022-01-01] = Existing[2022-12-25, 2022-12-30] & Existing[2022-12-31, 2023-01-05]
            // Result: Existing[2022-12-25, 2022-12-30], New[2022-12-26, 2023-01-01], Existing[2023-01-01 + 1 second, 2023-01-05]
            // Insert new record with validFrom = new record's effectiveTo + 1 second and validTo = existing record's effectiveTo

            log.info("Scenario 3: No Matching Records");

            String insertResult = insertBiTempData(createRequest);

            CreateResponse createResponse = new CreateResponse(
                    List.of(insertResult),
                    List.of()
            );

            return createResponse.toString();
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

    public BiTempObject getBiTempDataById(ObjectId id) {
        log.debug("Get BiTemp Data by Id: {}", id);

        Criteria matchId = Criteria.where("_id").is(id);

        final MatchOperation matchStage = Aggregation.match(matchId);

        final Aggregation aggregation = Aggregation.newAggregation(matchStage);

        AggregationResults<BiTempObject> aggregationResults =
                mongoTemplate.aggregate(aggregation, "forex", BiTempObject.class);

        return aggregationResults.getUniqueMappedResult();
    }

    public List<BiTempObject> getRelatedBiTempData(GetRequest getRequest) {
        log.debug("Get Related BiTemp Data: {}", getRequest);

        EffectiveMeta effectiveMetaQuery = new EffectiveMeta(
                getRequest.effectiveFrom().atOffset(zoneOffSet),
                getRequest.effectiveTo().atOffset(zoneOffSet));

        Long newFrom = effectiveMetaQuery.validFrom().toInstant().toEpochMilli();
        Long newTo = effectiveMetaQuery.validTo().toInstant().toEpochMilli();

        Criteria matchKey = Criteria.where("key").is(getRequest.id());

        Criteria matchEffectiveFromS = Criteria.where("effectiveMeta.validFrom.epochMilli").lte(newFrom);
        Criteria matchEffectiveFromE = Criteria.where("effectiveMeta.validTo.epochMilli").gte(newFrom);
        Criteria matchEffectiveFrom = new Criteria().andOperator(matchEffectiveFromS, matchEffectiveFromE);

        Criteria matchEffectiveToS = Criteria.where("effectiveMeta.validFrom.epochMilli").lte(newTo);
        Criteria matchEffectiveToE = Criteria.where("effectiveMeta.validTo.epochMilli").gte(newTo);
        Criteria matchEffectiveTo = new Criteria().andOperator(matchEffectiveToS, matchEffectiveToE);

        Criteria matchEffectiveFromBoundary = Criteria.where("effectiveMeta.validFrom.epochMilli").gt(newFrom);
        Criteria matchEffectiveToBoundary = Criteria.where("effectiveMeta.validTo.epochMilli").lt(newTo);
        Criteria matchEffectiveBoundary = new Criteria().andOperator(matchEffectiveFromBoundary, matchEffectiveToBoundary);

        Criteria matchEffective = new Criteria().orOperator(matchEffectiveFrom, matchEffectiveTo, matchEffectiveBoundary);

        Criteria matchActive = Criteria.where("isActive").is(true);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective, matchActive);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.ASC, "effectiveMeta.validFrom.epochMilli");

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
                true,
                null);
        log.debug("BiTemp Object: {}", biTempObject);
        return biTempObject;
    }

    public String deleteBiTempData(DeleteRequest deleteRequest) {
        log.debug("Delete BiTemp Data: {}", deleteRequest);

        GetRequest getRequest = new GetRequest(
                deleteRequest.key(),
                deleteRequest.effectiveFrom(),
                deleteRequest.effectiveTo()
        );

        //Get all related records
        List<BiTempObject> biTempObjects = getRelatedBiTempData(getRequest);

        //Should be only one record
        //Mark all related records as inactive
        if (biTempObjects.size() == 1) {
            BiTempObject biTempObject = biTempObjects.get(0);
            return markRecordAsInactive(biTempObject._id(), deleteRequest.deletedBy());
        } else {
            return "Invalid request";
        }
    }

    //Marking the record as inactive. Input is the ObjectId of the record
    public String markRecordAsInactive(ObjectId id, String deletedBy) {
        log.debug("Marking record as inactive: {}", id);

        Query query = new Query();
        query.addCriteria(Criteria.where("_id").is(id));

        Update update = new Update();
        update.set("isActive", false);
        update.set("recordMeta.deletedBy", deletedBy);
        update.set("recordMeta.deletedAt", OffsetDateTime.now());

        UpdateResult updateResult = mongoCollection.updateOne(
                query.getQueryObject(),
                update.getUpdateObject()
        );

        CreateResponse createResponse = new CreateResponse(
                List.of(),
                List.of(updateResult.toString())
        );

        return createResponse.toString();
    }

    //Convert FromEpochMilli, ToEpochMilli to EffectiveMeta
    private static EffectiveMeta convertEpochMilliToEffectiveMeta(Long fromEpochMilli, Long toEpochMilli) {
        OffsetDateTime fromDateTime = Instant.ofEpochMilli(fromEpochMilli).atOffset(zoneOffSet);
        OffsetDateTime toDateTime = Instant.ofEpochMilli(toEpochMilli).atOffset(zoneOffSet);
        return new EffectiveMeta(fromDateTime, toDateTime);
    }

}
