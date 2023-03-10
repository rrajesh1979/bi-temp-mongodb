package org.rrajesh1979.bitemp.service;

import com.mongodb.client.FindIterable;
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

    public CreateResponse createBiTempData(CreateRequest createRequest) {
        log.debug("Create BiTemp Data: {}", createRequest);

        //Get related BiTempData
        List<BiTempObject> relatedBiTempData = getRelatedBiTempData(
                new GetRequest(
                        createRequest.key(),
                        createRequest.effectiveFrom(),
                        createRequest.effectiveTo()
                )
        );
        log.debug("Related BiTemp Data: {}", relatedBiTempData);

        if (relatedBiTempData.size() == 1) {
            //Scenario 1: One Matching Record
            long newFrom = createRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
            long newTo = createRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

            long existingFrom = relatedBiTempData.get(0).effectiveMeta().effectiveFrom().toInstant().toEpochMilli();
            long existingTo = relatedBiTempData.get(0).effectiveMeta().effectiveTo().toInstant().toEpochMilli();

            Object existingData = relatedBiTempData.get(0).data();
            ObjectId existingId = relatedBiTempData.get(0)._id();

            if (newFrom == existingFrom && newTo == existingTo) {
                // Scenario 1.1: Exact Match. [NewFrom, NewTo] = [ExistingFrom, ExistingTo]
                // Ex: New[2022-12-25, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Update the existing record
                log.debug("Scenario 1.1: Exact Match. [NewFrom, NewTo] = [ExistingFrom, ExistingTo]");
                UpdateResult updateResult = updateBiTempData(existingId, createRequest.createdBy(), createRequest.data(), null);
                log.debug("Update Result: {}", updateResult);
                CreateResponse createResponse = new CreateResponse(
                        List.of(),
                        List.of(updateResult)
                );
                return createResponse;

            } else if (newFrom > existingFrom && newTo == existingTo ) {
                // Scenario 1.2: New From is after existing From and New To is equal to existing To
                // Ex: New[2022-12-26, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second] and New[2022-12-26, 2022-12-31]
                // Update the existing record's effectiveTo to new record's effectiveFrom - 1 second and insert new record
                log.debug("Scenario 1.2: New From is after existing From and New To is equal to existing To");
                UpdateResult updateResult = updateBiTempData(existingId, createRequest.createdBy(), null, convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000));

                log.debug("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult)
                );

                return createResponse;

            } else if (newFrom < existingFrom && newTo == existingTo) {
                //Scenario 1.3: New From is before existing From and New To is equal to existing To
                // Ex: New[2022-12-24, 2022-12-31] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-24, 2022-12-31]
                // Update the existing record's effectiveFrom to new record's effectiveFrom.
                // Update the existing record's data. No need to insert new record

                log.debug("Scenario 1.3: New From is before existing From and New To is equal to existing To");

                UpdateResult updateResult = updateBiTempData(existingId, createRequest.createdBy(), createRequest.data(), convertEpochMilliToEffectiveMeta(newFrom, existingTo));

                log.debug("Update Result: {}", updateResult);

                CreateResponse createResponse = new CreateResponse(
                        List.of(),
                        List.of(updateResult)
                );

                return createResponse;

            } else if (newFrom == existingFrom && newTo < existingTo) {
                //Scenario 1.4: New From is equal to existing From and New To is before existing To
                // Ex: New[2022-12-25, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-30 + 1 second, 2022-12-31] and New[2022-12-25, 2022-12-30]
                // Update the existing record's effectiveFrom to new record's effectiveTo + 1 second and insert new record

                log.debug("Scenario 1.4: New From is equal to existing From and New To is before existing To");

                UpdateResult updateResult = updateBiTempData(
                        existingId,
                        createRequest.createdBy(),
                        null,
                        convertEpochMilliToEffectiveMeta(newTo + 1000, existingTo)
                );

                log.debug("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult)
                );

                return createResponse;

            } else if (newFrom == existingFrom && newTo > existingTo) {
                //Scenario 1.5: New From is equal to existing From and New To is after existing To
                // Ex: New[2022-12-25, 2023-01-01] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: Existing[2022-12-25, 2022-12-31] D2 and New[2022-12-31 + 1 second, 2023-01-01] D1
                // Update the new record's effectiveFrom to existing record's effectiveTo + 1 second and insert new record

                log.debug("Scenario 1.5: New From is equal to existing From and New To is after existing To");
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

                return createResponse;

            } else if (newFrom > existingFrom && newTo < existingTo) {
                //Scenario 1.6: New From is after existing From and New To is before existing To
                // Ex: New[2022-12-26, 2022-12-30] D1 = Existing[2022-12-25, 2022-12-31] D2
                // Result: ExistingSplitA[2022-12-25, 2022-12-26 - 1 second] D2, New[2022-12-26, 2022-12-30] D1, ExistingSplitB[2022-12-30 + 1 second, 2022-12-31] D2
                // Update the existing record's effectiveTo to new record's effectiveFrom - 1 second and insert new record
                // Insert new record with effectiveFrom = new record's effectiveTo + 1 second and effectiveTo = existing record's effectiveTo

                log.debug("Scenario 1.6: New From is after existing From and New To is before existing To");

                UpdateResult updateResultA = updateBiTempData(
                        existingId,
                        createRequest.createdBy(),
                        null,
                        convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000)
                );

                log.debug("Update Result A: {}", updateResultA);

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
                        List.of(updateResultA)
                );

                return createResponse;

            } else if (newFrom < existingFrom && newTo > existingTo) {
                //Scenario 1.7: New From is before existing From and New To is after existing To
                // Ex: New[2022-12-24, 2023-01-01] = Existing[2022-12-25, 2022-12-31]
                // Result: NewSplitA[2022-12-24, 2022-12-25 - 1 second], Existing[2022-12-25, 2022-12-31], NewSplitB[2022-12-31 + 1 second, 2023-01-01]
                // Update the new record's effectiveTo to existing record's effectiveFrom - 1 second and insert new record
                // Insert new record with effectiveFrom = existing record's effectiveTo + 1 second and effectiveTo = new record's effectiveTo

                log.debug("Scenario 1.7: New From is before existing From and New To is after existing To");
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

                return createResponse;

            } else if (newFrom > existingFrom && newTo > existingTo) {
                //Scenario 1.8: New From is after existing From and New To is after existing To
                // Ex: New[2022-12-26, 2023-01-01] = Existing[2022-12-25, 2022-12-31]
                // Result: Existing[2022-12-25, 2022-12-26 - 1 second], New[2022-12-26, 2023-01-01]
                // Update the existing record's effectiveTo to new record's effectiveFrom - 1 second and insert new record

                log.debug("Scenario 1.8: New From is after existing From and New To is after existing To");

                UpdateResult updateResult = updateBiTempData(
                        existingId,
                        createRequest.createdBy(),
                        null,
                        convertEpochMilliToEffectiveMeta(existingFrom, newFrom - 1000)
                );

                log.debug("Update Result: {}", updateResult);

                String insertResult = insertBiTempData(createRequest);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult)
                );

                return createResponse;

            } else if (newFrom < existingFrom && newTo < existingTo) {
                //Scenario 1.9: New From is before existing From and New To is before existing To
                // Ex: New[2022-12-24, 2022-12-30] = Existing[2022-12-25, 2022-12-31]
                // Result: New[2022-12-24, 2022-12-30], Existing[2022-12-30 + 1 second, 2022-12-31]
                // Update the new record's effectiveTo to existing record's effectiveFrom - 1 second and insert new record

                log.debug("Scenario 1.9: New From is before existing From and New To is before existing To");

                String insertResult = insertBiTempData(createRequest);

                UpdateResult updateResult = updateBiTempData(
                        existingId,
                        createRequest.createdBy(),
                        null,
                        convertEpochMilliToEffectiveMeta(newTo + 1000, existingTo)
                );

                log.debug("Update Result: {}", updateResult);

                CreateResponse createResponse = new CreateResponse(
                        List.of(insertResult),
                        List.of(updateResult)
                );

                return createResponse;
            }

        } else if (relatedBiTempData.size() == 2) {
            //Scenario 2: Two Matching Records
            // Ex: New[2022-12-26, 2022-01-01] = Existing[2022-12-25, 2022-12-30] & Existing[2022-12-31, 2023-01-05]
            // Result: Existing[2022-12-25, 2022-12-26 - 1 second], New[2022-12-26, 2023-01-01], Existing[2023-01-01 + 1 second, 2023-01-05]
            // Update the existing record's effectiveTo to new record's effectiveFrom - 1 second and insert new record
            // Insert new record with effectiveFrom = new record's effectiveTo + 1 second and effectiveTo = existing record's effectiveTo

            log.debug("Scenario 2: Two Matching Records");

            long newFrom = createRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
            long newTo = createRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

            long existingFromA = relatedBiTempData.get(0).effectiveMeta().effectiveFrom().toInstant().toEpochMilli();
            long existingToA = newFrom - 1000;
            ObjectId existingIdA = relatedBiTempData.get(0)._id();

            long existingFromB = newTo + 1000;
            long existingToB = relatedBiTempData.get(1).effectiveMeta().effectiveTo().toInstant().toEpochMilli();
            ObjectId existingIdB = relatedBiTempData.get(1)._id();

            UpdateResult updateResultA = updateBiTempData(
                    existingIdA,
                    createRequest.createdBy(),
                    null,
                    convertEpochMilliToEffectiveMeta(existingFromA, existingToA)
            );

            String insertResult = insertBiTempData(createRequest);

            UpdateResult updateResultB = updateBiTempData(
                    existingIdB,
                    createRequest.createdBy(),
                    null,
                    convertEpochMilliToEffectiveMeta(existingFromB, existingToB)
            );

            CreateResponse createResponse = new CreateResponse(
                    List.of(insertResult),
                    List.of(updateResultA, updateResultB)
            );

            return createResponse;

        } else if (relatedBiTempData.size() > 2) {
            //Scenario Invalid: More than Two Matching Records. Avoid getting into this scenario
            return new CreateResponse(
                    List.of(),
                    List.of()
            );
        } else {
            //Scenario 3: No Matching Records
            // Ex: New[2022-12-26, 2022-01-01] = Existing[2022-12-25, 2022-12-30] & Existing[2022-12-31, 2023-01-05]
            // Result: Existing[2022-12-25, 2022-12-30], New[2022-12-26, 2023-01-01], Existing[2023-01-01 + 1 second, 2023-01-05]
            // Insert new record with effectiveFrom = new record's effectiveTo + 1 second and effectiveTo = existing record's effectiveTo

            log.debug("Scenario 3: No Matching Records");

            String insertResult = insertBiTempData(createRequest);

            CreateResponse createResponse = new CreateResponse(
                    List.of(insertResult),
                    List.of()
            );
            return createResponse;
        }

        return new CreateResponse(
                List.of(),
                List.of()
        );
    }

    private UpdateResult updateBiTempData(ObjectId existingId, String updatedBy, Object data, EffectiveMeta effectiveMeta) {
        Document updateDocument = new Document();
        if (data != null) {
            updateDocument.append("data", data);
        }

        if (effectiveMeta != null) {
            updateDocument.append("effectiveMeta", effectiveMeta);
        }

        //Update the updatedBy field
        updateDocument.append("recordMeta.updatedBy", updatedBy);

        //Update the updatedAt field
        updateDocument.append("recordMeta.updatedAt", OffsetDateTime.now(zoneOffSet));

        //Create a copy and then update the original document
        String copyResult = copyBiTempDate(existingId, updatedBy);

        UpdateResult updateResult = mongoCollection.updateOne(
                new Document("_id", existingId),
                new Document("$set", updateDocument)
                //TODO: Update UpdatedBy and UpdatedOn
        );

        return updateResult;
    }

    public String copyBiTempDate(ObjectId id, String updatedBy) {
        log.debug("Create a copy of the record before update!");
        BiTempObject existingBiTempObject = getBiTempDataById(id);
        BiTempObject newBiTempObject = new BiTempObject(
                    existingBiTempObject.key(),
                    existingBiTempObject.data(),
                    existingBiTempObject.recordMeta(),
                    existingBiTempObject.effectiveMeta(),
                    false,
                    null
                );
        //TODO: Update CreatedBy and CreatedOn
        Document biTempDocument = BiTempUtil.toBiTempObjectToDocument(newBiTempObject);

        InsertOneResult insertOneResult;
        try {
            insertOneResult = mongoCollection.insertOne(biTempDocument);
            return Objects.requireNonNull(insertOneResult.getInsertedId()).toString();
        } catch (Exception e) {
            log.error("Error while inserting the record: {}", e.getMessage());
            return "Error while inserting the record: " + e.getMessage();
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

    public List<Document> getBiTempDataAsOf(GetAsOfRequest getAsOfRequest) {
        log.debug("Get BiTemp Data As Of: {}", getAsOfRequest);

        Long asOf = getAsOfRequest.asOf().atOffset(zoneOffSet).toInstant().toEpochMilli();
        Long from = getAsOfRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
        Long to = getAsOfRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

        Criteria criteria = Criteria.where("key").is(getAsOfRequest.key())
                .and("recordMeta.updatedAt.epochMilli").lte(asOf)
                .and("effectiveMeta.effectiveFrom.epochMilli").lte(from)
                .and("effectiveMeta.effectiveTo.epochMilli").gte(to);

        final MatchOperation matchStage = Aggregation.match(criteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "recordMeta.updatedAt.epochMilli");
        final LimitOperation limitStage = Aggregation.limit(1);
        final Aggregation aggregation = Aggregation.newAggregation(matchStage, sortStage, limitStage);

        AggregationResults<Document> aggregationResults =
                mongoTemplate.aggregate(aggregation, "forex", Document.class);

        return new ArrayList<>(aggregationResults.getMappedResults());
    }

    public List<Document> getBiTempData(GetRequest getRequest) {
        log.debug("Get BiTemp Data: {}", getRequest);

        Long newFrom = getRequest.effectiveFrom().atOffset(zoneOffSet).toInstant().toEpochMilli();
        Long newTo = getRequest.effectiveTo().atOffset(zoneOffSet).toInstant().toEpochMilli();

        Criteria matchKey = Criteria.where("key").is(getRequest.key());

        //getRequest.effectiveFrom() between "effectiveMeta.effectiveFrom.dateTime" and "effectiveMeta.effectiveTo.dateTime"
        Criteria matchEffectiveFrom = Criteria.where("effectiveMeta.effectiveFrom.epochMilli").lte(newFrom);
        Criteria matchEffectiveTo = Criteria.where("effectiveMeta.effectiveTo.epochMilli").gte(newTo);

        Criteria matchEffective = new Criteria().andOperator(matchEffectiveFrom, matchEffectiveTo);

        Criteria matchActive = Criteria.where("isActive").is(true);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective, matchActive);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.DESC, "effectiveMeta.effectiveFrom.dateTime");

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

        //TODO: Simplify this
        EffectiveMeta effectiveMetaQuery = new EffectiveMeta(
                getRequest.effectiveFrom().atOffset(zoneOffSet),
                getRequest.effectiveTo().atOffset(zoneOffSet));

        Long newFrom = effectiveMetaQuery.effectiveFrom().toInstant().toEpochMilli();
        Long newTo = effectiveMetaQuery.effectiveTo().toInstant().toEpochMilli();

        Criteria matchKey = Criteria.where("key").is(getRequest.key());

        Criteria matchEffectiveFromS = Criteria.where("effectiveMeta.effectiveFrom.epochMilli").lte(newFrom);
        Criteria matchEffectiveFromE = Criteria.where("effectiveMeta.effectiveTo.epochMilli").gte(newFrom);
        Criteria matchEffectiveFrom = new Criteria().andOperator(matchEffectiveFromS, matchEffectiveFromE);

        Criteria matchEffectiveToS = Criteria.where("effectiveMeta.effectiveFrom.epochMilli").lte(newTo);
        Criteria matchEffectiveToE = Criteria.where("effectiveMeta.effectiveTo.epochMilli").gte(newTo);
        Criteria matchEffectiveTo = new Criteria().andOperator(matchEffectiveToS, matchEffectiveToE);

        Criteria matchEffectiveFromBoundary = Criteria.where("effectiveMeta.effectiveFrom.epochMilli").gt(newFrom);
        Criteria matchEffectiveToBoundary = Criteria.where("effectiveMeta.effectiveTo.epochMilli").lt(newTo);
        Criteria matchEffectiveBoundary = new Criteria().andOperator(matchEffectiveFromBoundary, matchEffectiveToBoundary);

        Criteria matchEffective = new Criteria().orOperator(matchEffectiveFrom, matchEffectiveTo, matchEffectiveBoundary);

        Criteria matchActive = Criteria.where("isActive").is(true);

        Criteria matchCriteria = new Criteria().andOperator(matchKey, matchEffective, matchActive);

        final MatchOperation matchStage = Aggregation.match(matchCriteria);
        final SortOperation sortStage = Aggregation.sort(Sort.Direction.ASC, "effectiveMeta.effectiveFrom.epochMilli");

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

    public CreateResponse deleteBiTempData(DeleteRequest deleteRequest) {
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
            return new CreateResponse(List.of(), List.of());
        }
    }

    //Marking the record as inactive. Input is the ObjectId of the record
    public CreateResponse markRecordAsInactive(ObjectId id, String deletedBy) {
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
                List.of(updateResult)
        );

        return createResponse;
    }

    //Convert FromEpochMilli, ToEpochMilli to EffectiveMeta
    private static EffectiveMeta convertEpochMilliToEffectiveMeta(Long fromEpochMilli, Long toEpochMilli) {
        OffsetDateTime fromDateTime = Instant.ofEpochMilli(fromEpochMilli).atOffset(zoneOffSet);
        OffsetDateTime toDateTime = Instant.ofEpochMilli(toEpochMilli).atOffset(zoneOffSet);
        return new EffectiveMeta(fromDateTime, toDateTime);
    }

}
