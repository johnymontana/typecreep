package com.trvrsl.typecreep;

import com.google.gson.Gson;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Hello world!
 *
 */
@Path("/typecreep")
public class NeoServerExtension
{
    private final ExecutionEngine executionEngine;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Gson gson_obj;

    public NeoServerExtension(@Context GraphDatabaseService database) {
        this.executionEngine = new ExecutionEngine(database);
        this.gson_obj = new Gson();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/insert")
    public Response insertData(String dataJson) throws Exception {
        TypeCreepData typeData = this.gson_obj.fromJson(dataJson, TypeCreepData.class);

        ArrayList<Object> resultsArray = new ArrayList<Object>();

        Map<String, Object> params = new HashMap<String, Object>();

        String user = typeData.getUser();

        params.put("user", user);
        Map<String, Object> first = typeData.getData().get(0);
        params.put("char", first.get("character"));
        String query =
                "MERGE (:LETTER {char: {char} })";

        // APPEND all other characters to query String

        for (Map<String, Object> obs : typeData.getData()) {
            String character = (String)obs.get("character");
            Integer timeUp = (Integer)obs.get("timeUp");
            Integer timeDown = (Integer)obs.get("timeDown");

            Integer duration = timeDown - timeUp;

            // TODO: get NEXT character duration

            query = query +
                    "-[:NEXT_CHAR {duration: " + duration.toString() + " }]->" +
                    "(:LETTER {char: " + character + ", duration: " + duration.toString()+" })";
        }
        query = query +
                "-[e:FROM_USER]->(u:User {id: {user} }) \n" +
                "ON CREATE SET e.count = 1 \n" +
                "ON MATCH SET coalesce(e.count, 0) + 1 \n" +
                "RETURN {user: u.id, count: e.count} as created";

        Iterator<Map<String, Object>> result = executionEngine.execute(query, params).iterator();
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            resultsArray.add(row.get("created"));
        }

        return Response.ok(objectMapper.writeValueAsString(resultsArray), MediaType.APPLICATION_JSON).build();





    }



}
