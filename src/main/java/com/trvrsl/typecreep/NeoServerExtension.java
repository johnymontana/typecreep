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

    public ArrayList<Object> insertWord(String user, String word, ArrayList<Map> characterList){
        ArrayList<Object> resultsArray = new ArrayList<Object>();

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("user", user);

        params.put("word", word);
        String query =
                "MERGE (:WORD {word: {word} })";


        for (Map<String, Object> obs : characterList) {
            String character = (String)obs.get("character");
            Long timeUp = ((Double)obs.get("timeUp")).longValue();
            Long timeDown = ((Double)obs.get("timeDown")).longValue();

            Long duration = timeUp - timeDown;

            // TODO: get NEXT character duration
            // TODO: split on spaces


            query = query +
                    "-[:NEXT_CHAR {duration: " + duration.toString() + " }]->" +
                    "(:LETTER {char: '" + character + "', duration: " + duration.toString()+" })";
        }
        query = query +
                "-[e:FROM_USER]->(u:User {id: {user} }) \n" +
                "ON CREATE SET e.count = 1 \n" +
                "ON MATCH SET e.count = coalesce(e.count, 0) + 1 \n" +
                "RETURN {user: u.id, count: e.count} as created";

        Iterator<Map<String, Object>> result = executionEngine.execute(query, params).iterator();
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            resultsArray.add(row.get("created"));
        }

        return resultsArray;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/insert")
    public Response insertData(String dataJson) throws Exception {


        TypeCreepData typeData = this.gson_obj.fromJson(dataJson, TypeCreepData.class);

        //ArrayList<Object> resultsArray = new ArrayList<Object>();

        String user = typeData.getUser();
        //String word = "placeholder";        // FIXME: get the actual word here
        ArrayList<Map> charList = typeData.getData();       // FIXME: split on spaces

        ArrayList<Map> words = new ArrayList<Map>();    // [{}]
        Map<String, Object> wordCharMap = new HashMap<String, Object>();


        ArrayList<Map> charWordList = new ArrayList<Map>();

        Integer count = 0;
        String word = "";
        //String user = "";

        Iterator<Map> charListIt = charList.iterator();

        while (charListIt.hasNext()){
            count++;
            Map<String, Object> character = charListIt.next();
            if (character.get("character").equals(" ")){

                // append charWordList to words
                // FIXME: keep track of word??, separate ArrayList?? (probably another map)
                // null out word
                // null out charWordList
                Map<String, Object> wordEntry = new HashMap<String, Object>();
                wordEntry.put("word", word);
                wordEntry.put("characters", charWordList);
                words.add(wordEntry);
                word = "";
                charWordList = new ArrayList<Map>();

            }

            else {
                // append character to charWordList
                // append ACTUAL character to word
                word = word + character.get("character");
                charWordList.add(character);
            }






        }

        ArrayList<Object> resultsArray = new ArrayList<Object>();
        for (Map<String, Object> w : words){
            resultsArray.add(insertWord(user, (String)w.get("word"), (ArrayList<Map>)w.get("characters") ));
        }
        //ArrayList<Object> resultsArray = insertWord(user, word, charList);



        return Response.ok(objectMapper.writeValueAsString(resultsArray), MediaType.APPLICATION_JSON).build();





    }



}
