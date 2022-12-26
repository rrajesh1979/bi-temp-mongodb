package org.rrajesh1979.bitemp.service;

import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.rrajesh1979.bitemp.config.MongoConfig;
import org.rrajesh1979.bitemp.model.CreateRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
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
class BiTempServiceTest {

    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:latest");

    static {
        mongoDBContainer.start();
    }

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    Environment env;

    private MongoConfig mongoConfig;
    private MongoCollection<Document> collection;

    private BiTempService biTempService;

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
    void setUpEach() {
        assert mongoTemplate != null;
        log.info("MongoTemplate created");
        mongoConfig = new MongoConfig(env);
        assertNotNull(mongoConfig);
        log.info("MongoConfig configured");
        collection = mongoConfig.getCollection();
        assertNotNull(collection);
        log.info("MongoCollection created");
        createIndexAndVerify();
        biTempService = new BiTempService(mongoTemplate, collection);
        assertNotNull(biTempService);
        log.info("BiTempService created");
    }

    private void createIndexAndVerify() {
        Document indexPattern = new Document();
        indexPattern.put("key", 1);
        indexPattern.put("effectiveMeta.validFrom.epochMilli", 1);
        indexPattern.put("effectiveMeta.validTo.epochMilli", 1);
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.name("key_validFrom_validTo");
        collection.createIndex(indexPattern, indexOptions);
        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().iterator().forEachRemaining(indexes::add);
        //Check if index is created
        assertTrue(
                indexes
                    .stream()
                    .anyMatch(
                            index -> index.getString("name").equals("key_validFrom_validTo")
                    )
        );
        log.info("Index created");
    }

    @AfterEach
    void tearDown() {
        log.info("Cleaning up after test");
    }

    static class ArrayOfCreateRequest {
        CreateRequest[] createRequests;
    }

    @Test
    void createBiTempData() throws IOException {
        assertNotNull(collection);

        //Read data from test_data.json
        List<CreateRequest> createRequests = new ArrayList<>();
        buildTestData(createRequests);
        //Create data in collection
        createRequests.forEach(biTempService::createBiTempData);
        //Verify data in collection
        assertEquals(10, collection.countDocuments());


        CreateRequest newCreateRequest = new CreateRequest(
                """
                        {
                        "from": "USD",
                        "to": "INR"
                        }
                        """,
                """
                        {
                        "exchangeRate": 81.9379
                        }
                        """,
                "rrajesh1979",
                LocalDateTime.of(2021, 12, 1, 0, 0, 0),
                LocalDateTime.of(2021, 12, 31, 0, 0, 0)
        );

        String id = biTempService.createBiTempData(newCreateRequest);
        log.info("Created new document with id: {}", id);
        assertNotNull(id);

    }

    private static void buildTestData(List<CreateRequest> createRequests) throws IOException {
        Gson gson = new Gson();
        Reader reader = Files.newBufferedReader(Paths.get("/Users/rajesh/Learn/bi-temp-mongodb/bi-temp-persist/src/test/resources/data/test_data.json"));
        List list = gson.fromJson(reader, List.class);
        list.forEach(
                item -> {
                    Map<String, Object> map = (Map<String, Object>) item;
                    CreateRequest createRequest = new CreateRequest(
                            map.get("key").toString(),
                            map.get("data").toString(),
                            map.get("createdBy").toString(),
                            LocalDateTime.parse(map.get("effectiveFrom").toString()),
                            LocalDateTime.parse(map.get("effectiveTo").toString())
                    );
                    log.info("CreateRequest: {}", createRequest);
                    createRequests.add(createRequest);
                }
        );
        log.info("Read test data from test_data.json");
    }
}