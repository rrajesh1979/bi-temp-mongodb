package org.rrajesh1979.bitemp.service;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.rrajesh1979.bitemp.config.MongoConfig;
import org.rrajesh1979.bitemp.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Log4j2
@DataMongoTest(properties = {
        "spring.data.mongodb.database=bitemp",
        "spring.data.mongodb.collection=forex"
})
@Testcontainers
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BiTempServiceTest {

    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:latest");

    public static final ZoneOffset zoneOffSet= ZoneOffset.of("-05:00");

    static final String TEST_DATA_BASE = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_base.json";
    static final String TEST_DATA_S2 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s2.json";
    static final String TEST_DATA_S1_1 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_1.json";
    static final String TEST_DATA_S1_2 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_2.json";
    static final String TEST_DATA_S1_3 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_3.json";
    static final String TEST_DATA_S1_4 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_4.json";
    static final String TEST_DATA_S1_5 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_5.json";
    static final String TEST_DATA_S1_6 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_6.json";
    static final String TEST_DATA_S1_7 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_7.json";
    static final String TEST_DATA_S1_8 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_8.json";
    static final String TEST_DATA_S1_9 = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_s1_9.json";
    static final String TEST_DATA_DELETE = "/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data_delete.json";

    static {
        mongoDBContainer.start();
    }

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    Environment env;

    private MongoCollection<Document> collection;

    private BiTempService biTempService;

    List<CreateRequest> baseDataCreateRequests = new ArrayList<>();

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @BeforeAll
    static void setUp() {
        assert mongoDBContainer.isRunning();
        log.info("MongoDBContainer started");
    }

    @BeforeEach
    void setUpEach() throws IOException {
        assert mongoTemplate != null;
        log.info("MongoTemplate created");
        MongoConfig mongoConfig = new MongoConfig(env);
        assertNotNull(mongoConfig);
        log.info("MongoConfig configured");
        collection = mongoConfig.getCollection();
        assertNotNull(collection);
        log.info("MongoCollection created");
        createIndexAndVerify();
        biTempService = new BiTempService(mongoTemplate, collection);
        assertNotNull(biTempService);
        log.info("BiTempService created");

        //Read data from test_data_base.json
        buildTestData(baseDataCreateRequests, TEST_DATA_BASE);
        //Create data in collection
        baseDataCreateRequests.forEach(biTempService::createBiTempData);
    }

    private void createIndexAndVerify() {
        Document indexPattern = new Document();
        indexPattern.put("key", 1);
        indexPattern.put("effectiveMeta.effectiveFrom.epochMilli", 1);
        indexPattern.put("effectiveMeta.effectiveTo.epochMilli", 1);
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.name("key_effectiveFrom_effectiveTo");
        collection.createIndex(indexPattern, indexOptions);
        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().iterator().forEachRemaining(indexes::add);
        //Check if index is created
        assertTrue(
                indexes
                    .stream()
                    .anyMatch(
                            index -> index.getString("name").equals("key_effectiveFrom_effectiveTo")
                    )
        );
        log.info("Index created");

    }

    @AfterEach
    void tearDown() {
        log.info("Cleaning up after test");
        collection.drop();
    }

    private void buildTestData(List<CreateRequest> createRequests, String testFilePath) throws IOException {
        //Roundabout way to read json array from file to avoid Spring Bug

        Gson gson = new Gson();
        Reader reader = Files.newBufferedReader(Paths.get(testFilePath));
        List list = gson.fromJson(reader, List.class);
        list.forEach(
                item -> {
                    Map<String, Object> map = (Map<String, Object>) item;
                    CreateRequest createRequest = new CreateRequest(
                            map.get("key").toString(),
                            map.get("data") == null ? null : map.get("data").toString(), //Check for null
                            map.get("createdBy") == null ? null : map.get("createdBy").toString(), //Check for null
                            LocalDateTime.parse(map.get("effectiveFrom").toString()),
                            LocalDateTime.parse(map.get("effectiveTo").toString())
                    );
//                    log.info("CreateRequest: {}", createRequest);
                    createRequests.add(createRequest);
                }
        );
        log.info("Read test data from test_data_base.json");
    }

    @Test
    @Order(1)
    void createBiTempData() {
        assertNotNull(collection);

        //Verify data in collection
        assertEquals(12, collection.countDocuments());
        log.info("Data created and verified");

    }

    @Setter @Getter @AllArgsConstructor
    static
    class DataKey {
        private Object from;
        private Object to;
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 2: New data overlaps two different existing records")
    void testScenario2() throws IOException {
        //Read data from test_data_base.json
        buildTestData(baseDataCreateRequests, TEST_DATA_BASE);

        List<CreateRequest> sceanrio1CreateRequests = new ArrayList<>();
        buildTestData(sceanrio1CreateRequests, TEST_DATA_S2);

        //Create new data
        CreateRequest newCreateRequest = sceanrio1CreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();
        ObjectId relatedIdB = relatedBiTempObjects.get(1)._id();

        log.info("Related BiTempObjects: {}", relatedBiTempObjects);
        assertEquals(2, relatedBiTempObjects.size());
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        assertEquals(13, collection.countDocuments());

        //Verify updates to related objects
        BiTempObject updatedBiTempObjectA = biTempService.getBiTempDataById(relatedIdA);
        BiTempObject updatedBiTempObjectB = biTempService.getBiTempDataById(relatedIdB);
        log.info("Updated BiTempObject A: {}", updatedBiTempObjectA);
        log.info("Updated BiTempObject B: {}", updatedBiTempObjectB);

        assert updatedBiTempObjectA
                .effectiveMeta()
                    .effectiveTo()
                    .toInstant()
                    .toEpochMilli()
                ==
                newCreateRequest
                        .effectiveFrom()
                        .toInstant(zoneOffSet)
                        .toEpochMilli()
                - 1000;

        assert updatedBiTempObjectB
                .effectiveMeta()
                    .effectiveFrom()
                    .toInstant()
                    .toEpochMilli()
                ==
                newCreateRequest
                        .effectiveTo()
                        .toInstant(zoneOffSet)
                        .toEpochMilli()
                + 1000;

    }

    @Test
    @Order(3)
    @DisplayName("Scenario 1.1: New data effective period matches exactly one existing record")
    void testScenario1_1() throws IOException {
        //Arrange
        //Setup base data set. Read data set for testing scenario
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> sceanrioCreateRequests = new ArrayList<>();
        buildTestData(sceanrioCreateRequests, TEST_DATA_S1_1);

        CreateRequest newCreateRequest = sceanrioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore, countAfter);
        assertEquals(newCreateRequest.data(), updatedBiTempObject.data());
    }

    @Test
    @Order(4)
    @DisplayName("Scenario 1.2: New data effectiveFrom > existing record effectiveFrom and effectiveTo = existing record effectiveTo")
    void testScenario1_2() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> sceanrioCreateRequests = new ArrayList<>();
        buildTestData(sceanrioCreateRequests, TEST_DATA_S1_2);
        CreateRequest newCreateRequest = sceanrioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 1, countAfter);
        assertEquals(updatedBiTempObject.effectiveMeta().effectiveTo().toInstant().toEpochMilli(),
                newCreateRequest.effectiveFrom().toInstant(zoneOffSet).toEpochMilli() - 1000);
    }

    @Test
    @Order(5)
    @DisplayName("Scenario 1.3: New data effectiveFrom < existing record effectiveFrom and effectiveTo = existing record effectiveTo")
void testScenario1_3() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_3);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore, countAfter);
        assertEquals(updatedBiTempObject.effectiveMeta().effectiveFrom().toInstant().toEpochMilli(),
                newCreateRequest.effectiveFrom().toInstant(zoneOffSet).toEpochMilli());
        assertEquals(updatedBiTempObject.data(), newCreateRequest.data());
    }

    @Test
    @Order(6)
    @DisplayName("Scenario 1.4: New data effectiveFrom = existing record effectiveFrom and effectiveTo < existing record effectiveTo")
    void testScenario1_4() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_4);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 1, countAfter);
        assertEquals(updatedBiTempObject.effectiveMeta().effectiveFrom().toInstant().toEpochMilli(),
                newCreateRequest.effectiveTo().toInstant(zoneOffSet).toEpochMilli() + 1000);
    }

    @Test
    @Order(7)
    @DisplayName("Scenario 1.5: New data effectiveFrom = existing record effectiveFrom and effectiveTo > existing record effectiveTo")
    void testScenario1_5() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_5);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 1, countAfter);
        //TODO: Get new data inserted and add assertion

    }

    @Test
    @Order(8)
    @DisplayName("Scenario 1.6: New data effectiveFrom > existing record effectiveFrom and effectiveTo < existing record effectiveTo")
    void testScenario1_6() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_6);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 2, countAfter);
    }

    @Test
    @Order(9)
    @DisplayName("Scenario 1.7: New data effectiveFrom < existing record effectiveFrom and effectiveTo > existing record effectiveTo")
    void testScenario1_7() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_7);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 2, countAfter);
    }

    @Test
    @Order(10)
    @DisplayName("Scenario 1.8: New data effectiveFrom > existing record effectiveFrom and effectiveTo > existing record effectiveTo")
    void testScenario1_8() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_8);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 1, countAfter);

        assertEquals(updatedBiTempObject.effectiveMeta().effectiveTo().toInstant().toEpochMilli(),
                newCreateRequest.effectiveFrom().toInstant(zoneOffSet).toEpochMilli() - 1000);
    }

    @Test
    @Order(11)
    @DisplayName("Scenario 1.9: New data effectiveFrom < existing record effectiveFrom and effectiveTo < existing record effectiveTo")
    void testScenario1_9() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioCreateRequests = new ArrayList<>();
        buildTestData(scenarioCreateRequests, TEST_DATA_S1_9);
        CreateRequest newCreateRequest = scenarioCreateRequests.get(0);
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newCreateRequest.key(),
                        newCreateRequest.effectiveFrom(),
                        newCreateRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.createBiTempData(newCreateRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore + 1, countAfter);

        assertEquals(updatedBiTempObject.effectiveMeta().effectiveFrom().toInstant().toEpochMilli(),
                newCreateRequest.effectiveTo().toInstant(zoneOffSet).toEpochMilli() + 1000);
    }

    @Test
    @Order(12)
    @DisplayName("Delete / Mark as inactive")
    void testDelete() throws IOException {
        //Arrange
        //Get count before test
        //Get related objects before test
        long countBefore = collection.countDocuments();

        List<CreateRequest> scenarioDeleteRequests = new ArrayList<>();
        buildTestData(scenarioDeleteRequests, TEST_DATA_DELETE);
        DeleteRequest newDeleteRequest = new DeleteRequest(
                scenarioDeleteRequests.get(0).key(),
                scenarioDeleteRequests.get(0).createdBy(),
                scenarioDeleteRequests.get(0).effectiveFrom(),
                scenarioDeleteRequests.get(0).effectiveTo()
        );
        List<BiTempObject> relatedBiTempObjects = biTempService.getRelatedBiTempData(
                new GetRequest(
                        newDeleteRequest.key(),
                        newDeleteRequest.effectiveFrom(),
                        newDeleteRequest.effectiveTo()
                )
        );
        ObjectId relatedIdA = relatedBiTempObjects.get(0)._id();

        //Act
        //Create new data
        CreateResponse result = biTempService.deleteBiTempData(newDeleteRequest);
        BiTempObject updatedBiTempObject = biTempService.getBiTempDataById(relatedIdA);

        //Assert
        long countAfter = collection.countDocuments();

        assert relatedBiTempObjects.size() == 1;
        assertEquals(countBefore, countAfter);

        assertFalse(updatedBiTempObject.isActive());
    }

}