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

import java.util.*;


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

    /** classifyWord
     *
     * @param word
     * @param characterList
     * @return
     */
    public ArrayList<Object> classifyWord(String word, ArrayList<Map> characterList) {
        ArrayList<Object> resultsArray = new ArrayList<Object>();

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("word", word);

        String query =
                "MATCH p=(:WORD {word: {word} })-[r*]-(:User) RETURN "+
                "{user: [x IN nodes(p) WHERE 'User' IN labels(x) | x.id], " +
                "interDurations:[x IN rels(p) WHERE type(x)='NEXT_CHAR' | x.duration], " +
                "charDurations: [x IN nodes(p) WHERE 'LETTER' IN labels(x) | x.duration], word: {word}} AS observations";

        Long sumDuration = ((Double)0.0).longValue();
        Long sumInter = ((Double)0.0).longValue();

        for (Map<String, Object> obs : characterList){
            String character = (String)obs.get("character");
            Long timeUp = ((Double)obs.get("timeUp")).longValue();
            Long timeDown = ((Double)obs.get("timeDown")).longValue();

            Long duration = timeUp - timeDown;
            Long interDuration = (Long)obs.get("interDuration");

            sumDuration += duration;
            sumInter += interDuration;

        }


        Iterator<Map<String, Object>> result = executionEngine.execute(query, params).iterator();
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            //resultsArray.add(row.get("observations"));
            Map<String, Object> obs = (Map)row.get("observations");
            Long obsSumDuration = ((Double)0.0).longValue();
            Long obsSumInter = ((Double)0.0).longValue();

            List<Long> inters = (List)obs.get("interDurations");
            for (Long x : inters){
                obsSumInter += x;
            }

            List<Long> charDurs = (List)obs.get("charDurations");
            for (Long x : charDurs){
                obsSumDuration += x;
            }

            Map<String, Object> resultMap = new HashMap<String, Object>();
            resultMap.put("user", obs.get("user"));
            resultMap.put("word", obs.get("word"));
            resultMap.put("interDiff", sumInter-obsSumInter);
            resultMap.put("charDiff", sumDuration-obsSumDuration);
            resultsArray.add(resultMap);

        }
        return resultsArray;
    }

    /** insertWord
     *
     * @param user
     * @param word
     * @param characterList
     * @return
     */
    public ArrayList<Object> insertWord(String user, String word, ArrayList<Map> characterList){
        ArrayList<Object> resultsArray = new ArrayList<Object>();

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("user", user);

        params.put("word", word);
        String query =
                "MERGE (u:User {id: {user} }) \n" +
                "MERGE (:WORD {word: {word} })";


        for (Map<String, Object> obs : characterList) {
            String character = (String)obs.get("character");
            Long timeUp = ((Double)obs.get("timeUp")).longValue();
            Long timeDown = ((Double)obs.get("timeDown")).longValue();

            Long duration = timeUp - timeDown;

            Long interDuration = (Long)obs.get("interDuration");

            // TODO: get NEXT character duration
            // TODO: split on spaces


            query = query +
                    "-[:NEXT_CHAR {duration: " + interDuration.toString() + " }]->" +
                    "(:LETTER {char: '" + character + "', duration: " + duration.toString()+" })";
        }
        query = query +
                "-[e:FROM_USER]->(u) \n" +
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

    /** classifyData
     *
     * @param dataJson
     * @return
     * @throws Exception
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/classify")
    public Response classifyData(String dataJson) throws Exception {
        TypeCreepData typeData = this.gson_obj.fromJson(dataJson, TypeCreepData.class);

        ArrayList<Map> charList = typeData.getData();
        ArrayList<Map> words = new ArrayList<Map>();
        ArrayList<Map> charWordList = new ArrayList<Map>();

        Integer count = 0;
        String word = "";

        Iterator<Map> charListIt = charList.iterator();

        while (charListIt.hasNext()) {
            count++;
            Map<String, Object> character = charListIt.next();
            if (character.get("character").equals(" ")){
                Map<String, Object> wordEntry = new HashMap<String, Object>();
                wordEntry.put("word", word);
                wordEntry.put("characters", charWordList);
                words.add(wordEntry);
                word = "";
                charWordList = new ArrayList<Map>();
            } else {
                Long duration;
                if (charListIt.hasNext()) {
                    Map<String, Object> nextChar = charList.get(count);
                    Long timeUp = ((Double) character.get("timeUp")).longValue();
                    Long timeDown = ((Double) nextChar.get("timeDown")).longValue();

                    duration = timeDown - timeUp;
                } else {
                    duration = ((Double)0.0).longValue();
                }

                character.put("interDuration", duration);
                word = word + character.get("character");
                charWordList.add(character);
            }
        }

        ArrayList<Object> resultsArray = new ArrayList<Object>();
        for (Map<String, Object> w: words) {
            resultsArray.add(classifyWord((String)w.get("word"), (ArrayList<Map>)w.get("characters")));

        }

        // TODO: write function to aggregate results from classifyWord

        return Response.ok(objectMapper.writeValueAsString(resultsArray), MediaType.APPLICATION_JSON).build();


    }


    /** insertData
     *
     * @param dataJson
     * @return
     * @throws Exception
     */
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
                // calculate inter-character duration here, add to character map

                Long duration;
                if (charListIt.hasNext()) {
                    Map<String, Object> nextChar = charList.get(count);

                    Long timeUp = ((Double) character.get("timeUp")).longValue();
                    Long timeDown = ((Double) nextChar.get("timeDown")).longValue();

                    duration = timeDown - timeUp;
                }
                else {
                    duration = ((Double)0.0).longValue();
                }

                character.put("interDuration", duration);

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
