package org.hackunix.webhook;

import io.pravega.cdi.PravegaConfig;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.TestUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.inject.Inject;

@QuarkusTest
public class WebhookResourceTest {

    @Inject
    @PravegaConfig(scope="webhooks", stream="webhook", serializer=UTF8StringSerializer.class)
    EventStreamReader<String> webhookReader;

    @Inject
    @PravegaConfig(scope="webhooks", stream="testStream", serializer=UTF8StringSerializer.class)
    EventStreamReader<String> streamReader;

    @Inject
    @PravegaConfig(scope="testScope", stream="testStream", serializer=UTF8StringSerializer.class)
    EventStreamReader<String> scopeAndStreamReader;

    static LocalPravegaEmulator localPravega;

    @BeforeAll
    public static void launchPravegaCluster() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        localPravega = LocalPravegaEmulator.builder()
                .controllerPort(9090)
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .enableRestServer(false)
                .enableAuth(false)
                .enableTls(false)
                .build();
        Method startMethod = localPravega.getClass().getDeclaredMethod("start");
        startMethod.setAccessible(true);
        startMethod.invoke(localPravega);
    }

    @AfterAll
    public static void shutdown() throws Exception {
        localPravega.close();
    }

    @Test
    public void testWebhookEndpoint() {
        final String jsonBody = "{\"webhooks\":\"webhook\"}";
        whenPostExpectBodyEchoedAndStreamed("/webhook", jsonBody, webhookReader);
    }

    @Test
    public void testWebhookScopeAndStreamEndpoint() {
        final String jsonBody = "{\"test\":\"test\"}";
        whenPostExpectBodyEchoedAndStreamed("/webhook/testScope/testStream", jsonBody, scopeAndStreamReader);
    }

    @Test
    public void testWebhookStreamEndpoint() {
        final String jsonBody = "{\"webhooks\":\"test\"}";
        whenPostExpectBodyEchoedAndStreamed("/webhook/testStream", jsonBody, streamReader);
    }

    private void whenPostExpectBodyEchoedAndStreamed(String path, String jsonBody, EventStreamReader<String> reader) {
        given()
          .contentType(ContentType.JSON)
          .body(jsonBody)
          .when().post(path)
          .then()
            .statusCode(200)
            .body(is(jsonBody));
        assertEquals(jsonBody, reader.readNextEvent(1000).getEvent());
    }

}
