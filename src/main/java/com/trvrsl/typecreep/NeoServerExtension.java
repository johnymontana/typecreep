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

    /** insertSampleQuery
     *
     * @param charList - list of characters in the sample
     * @param charTimings - timings for characters themselves
     * @param interTimings - timings between characters
     * @return the resulting query
     */
    public String insertSampleQuery(ArrayList<String> charList, ArrayList<Integer> charTimings,
                             ArrayList<Integer> interTimings) {
        // Build the character list for the entire sample
        String query = "MERGE ";
        for (int i = 0; i < charList.size(); i++) {
            String nextChar = charList.get(i);
            String nextCharDuration = charTimings.get(i).toString();
            query = query + "(:Letter { char: '" + nextChar + "', duration: " + nextCharDuration + " })";
            if (i < (charList.size() - 1)) {
                String nextInterDuration = interTimings.get(i).toString();
                query = query + "-[:NEXT_CHAR { duration: " + nextInterDuration + " }]->";
            }
        }
        // Add the user
        query = query + "-[:FROM_USER]->(:User { id: {user} })";

        return query;
    }

    /** insertSample
     *
     * @param user - the user name to associate with the data
     * @param charList - list of characters in the sample
     * @param charTimings - timings for characters themselves
     * @param interTimings - timings between characters
     */
    public void insertSample(String user, ArrayList<String> charList, ArrayList<Integer> charTimings,
                               ArrayList<Integer> interTimings) {
        String query = this.insertSampleQuery(charList, charTimings, interTimings);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("user", user);
        executionEngine.execute(query, params);
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

        String user = typeData.getUser();

        ArrayList<String> charList = new ArrayList<String>();
        ArrayList<Integer> charTimings = new ArrayList<Integer>();
        ArrayList<Integer> interTimings = new ArrayList<Integer>();

        ArrayList<Map> dataList = typeData.getData();
        for (int i = 0; i < dataList.size(); i++) {
            Map datum = dataList.get(i);
            charList.add((String) datum.get("character"));
            Integer keyDown = ((Double) datum.get("timeDown")).intValue();
            Integer keyUp = ((Double) datum.get("timeUp")).intValue();
            Integer delta = keyUp - keyDown;
            charTimings.add(delta);
            if (i > 0) {
                Map lastDatum = dataList.get(i-1);
                Integer lastKeyUp = ((Double) lastDatum.get("timeUp")).intValue();
                Integer interDelta = keyDown - lastKeyUp;
                interTimings.add(interDelta);
            }
        }

        this.insertSample(user, charList, charTimings, interTimings);

        return Response.ok().build();
    }
}
