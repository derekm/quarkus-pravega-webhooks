package org.hackunix.webhook;

import java.util.concurrent.ExecutionException;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.pravega.cdi.PravegaConfig;
import io.pravega.cdi.PravegaConfigQualifier;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.UTF8StringSerializer;

@Path("/webhook")
public class WebhookResource {

    @Inject
    Jsonb jsonb;

    @Inject
    @PravegaConfig(scope = "webhooks", stream = "webhook", serializer = UTF8StringSerializer.class)
    EventStreamWriter<String> writer;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject post(JsonObject json) throws JsonbException, InterruptedException, ExecutionException {
        writer.writeEvent(jsonb.toJson(json)).get();
        return json;
    }

    @Path("{stream}")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject post(@PathParam("stream") String stream, JsonObject json) {
        return post("webhooks", stream, json);
    }

    @Path("{scope}/{stream}")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject post(@PathParam("scope") String scope, @PathParam("stream") String stream, JsonObject json) {
        @SuppressWarnings("rawtypes")
        Instance<EventStreamWriter> writerInst = CDI.current()
                .select(EventStreamWriter.class, PravegaConfigQualifier.builder()
                        .scope(scope)
                        .stream(stream)
                        .serializer(UTF8StringSerializer.class)
                        .build());
        @SuppressWarnings("unchecked")
        EventStreamWriter<String> writer = writerInst.get();
        writer.writeEvent(jsonb.toJson(json));
        writerInst.destroy(writer);
        return json;
    }

}
