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



    public Map<String, Object> classifySample(Map<String,ArrayList<Double>> sample){

        ArrayList<String> grams = new ArrayList<String>();
        grams.addAll(sample.keySet());

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("sample", grams);

        String query =
                "MATCH (a:Letter)-[k]-(b:Letter)-[*]->(u:User)\n" +
                        "WHERE (a.char+b.char) IN {sample} \n" +
                        "WITH (a.char+b.char) AS gram, a, b, u, k\n" +
                        "WITH avg(a.duration) as avg_a, stdev(a.duration) as stdev_a, avg(b.duration) as avg_b, avg(k.duration) as avg_k, u, gram, count(k) as n, stdev(b.duration) as stdev_b, stdev(k.duration) as stdev_k\n" +
                        "RETURN gram, collect({a: avg_k, stdev_a: stdev_a, b: avg_k, k: avg_k, user:u.id, n: n, stdev_b: stdev_b, stdev_k: stdev_k}) as obs";

        Map<String,ArrayList<Map<String, Object>>> resultsMap = new HashMap<String, ArrayList<Map<String, Object>>>();

        Iterator<Map<String, Object>> result = executionEngine.execute(query, params).iterator();
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            ArrayList<Map<String, Object>> obs = (ArrayList<Map<String, Object>>)row.get("obs");
            String gram = (String)row.get("gram");
            resultsMap.put(gram, obs);
        }

        // classified_userse = {User.id -> avg squared errors}

        Map<String, Object> classified_users = new HashMap<String, Object>();

        for (Map.Entry<String, ArrayList<Double>> entry : sample.entrySet()) {
            String key = entry.getKey();
            ArrayList<Double> sample_vector = entry.getValue();

            if (resultsMap.get(key) != null){
                // there is at least one observation for the sample
                // for each matching sample, calculate the squared errors and update classified_users map
                for (Map<String, Object> o : resultsMap.get(key)) {
                    String user = (String) o.get("user");
                    ArrayList<Double> obs_vector = (ArrayList<Double>) o.get("vector");

                    ArrayList<Double> errors = new ArrayList<Double>();
                    for (int i=0; i < obs_vector.size(); i++){
                        obs_vector.add(i, Math.pow(obs_vector.get(i)-sample_vector.get(i), 2));
                    }

                    // FIXME: check if user exists in classified_users keys and increment errors instead of overwrite
                    classified_users.put(user, errors);

                }
            }



        }

        return classified_users;
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
    public String insertSampleQuery(ArrayList<String> charList, ArrayList<Long> charTimings,
                             ArrayList<Long> interTimings) {
        // Build the character list for the entire sample
        String query = "MERGE ";
        for (int i = 0; i < charList.size(); i++) {
            String nextChar = charList.get(i);
            if (nextChar.equals("'")) {
                nextChar = "AP";
            }
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
    public void insertSample(String user, ArrayList<String> charList, ArrayList<Long> charTimings,
                               ArrayList<Long> interTimings) {
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

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/classify")

    public Response classifyData(String dataJson) throws Exception {
        TypeCreepData typeData = this.gson_obj.fromJson(dataJson, TypeCreepData.class);

        ArrayList<String> charList = getSampleCharList(typeData.getData());
        ArrayList<Long> charTimings = getSampleCharTimings(typeData.getData());
        ArrayList<Long> interTimings = getSampleInterTimings(typeData.getData());

        Map<String,ArrayList<Double>> sampleNGrams = getSampleNGrams(charList, charTimings, interTimings, 2);

        Map<String,Object> classifiedUsers = classifySample(sampleNGrams);

        return Response.ok(objectMapper.writeValueAsString(classifiedUsers), MediaType.APPLICATION_JSON).build();
    }

//    /** classifyData
//     *
//     * @param dataJson
//     * @return
//     * @throws Exception
//     */
//    @POST
//    @Consumes(MediaType.APPLICATION_JSON)
//    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/classify")
//    public Response classifyData(String dataJson) throws Exception {
//        TypeCreepData typeData = this.gson_obj.fromJson(dataJson, TypeCreepData.class);
//
//        ArrayList<Map> charList = typeData.getData();
//        ArrayList<Map> words = new ArrayList<Map>();
//        ArrayList<Map> charWordList = new ArrayList<Map>();
//
//        Integer count = 0;
//        String word = "";
//
//        Iterator<Map> charListIt = charList.iterator();
//
//        while (charListIt.hasNext()) {
//            count++;
//            Map<String, Object> character = charListIt.next();
//            if (character.get("character").equals(" ")){
//                Map<String, Object> wordEntry = new HashMap<String, Object>();
//                wordEntry.put("word", word);
//                wordEntry.put("characters", charWordList);
//                words.add(wordEntry);
//                word = "";
//                charWordList = new ArrayList<Map>();
//            } else {
//                Long duration;
//                if (charListIt.hasNext()) {
//                    Map<String, Object> nextChar = charList.get(count);
//                    Long timeUp = ((Double) character.get("timeUp")).longValue();
//                    Long timeDown = ((Double) nextChar.get("timeDown")).longValue();
//
//                    duration = timeDown - timeUp;
//                } else {
//                    duration = ((Double)0.0).longValue();
//                }
//
//                character.put("interDuration", duration);
//                word = word + character.get("character");
//                charWordList.add(character);
//            }
//        }
//
//        ArrayList<Object> resultsArray = new ArrayList<Object>();
//        for (Map<String, Object> w: words) {
//            resultsArray.add(classifyWord((String)w.get("word"), (ArrayList<Map>)w.get("characters")));
//
//        }
//
//        // TODO: write function to aggregate results from classifyWord
//
//        return Response.ok(objectMapper.writeValueAsString(resultsArray), MediaType.APPLICATION_JSON).build();
//
//
//    }

    /** getSampleCharList
     *
     * @param dataList - the data to parse
     */
    public ArrayList<String> getSampleCharList(ArrayList<Map> dataList) {
        ArrayList<String> charList = new ArrayList<String>();

        for (int i = 0; i < dataList.size(); i++) {
            Map datum = dataList.get(i);
            charList.add((String) datum.get("character"));
        }

        return charList;
    }

    /** getSampleCharTimings
     *
     * @param dataList - the data to parse
     */
    public ArrayList<Long> getSampleCharTimings(ArrayList<Map> dataList) {
        ArrayList<Long> charTimings = new ArrayList<Long>();

        for (int i = 0; i < dataList.size(); i++) {
            Map datum = dataList.get(i);
            Long keyDown = ((Double) datum.get("timeDown")).longValue();
            Long keyUp = ((Double) datum.get("timeUp")).longValue();
            Long delta = keyUp - keyDown;
            charTimings.add(delta);
        }

        return charTimings;
    }

    /** getSampleInterTimings
     *
     * @param dataList - the data to parse
     */
    public ArrayList<Long> getSampleInterTimings(ArrayList<Map> dataList) {
        ArrayList<Long> interTimings = new ArrayList<Long>();

        for (int i = 0; i < dataList.size(); i++) {
            Map datum = dataList.get(i);
            Long keyDown = ((Double) datum.get("timeDown")).longValue();
            if (i > 0) {
                Map lastDatum = dataList.get(i-1);
                Long lastKeyUp = ((Double) lastDatum.get("timeUp")).longValue();
                Long interDelta = keyDown - lastKeyUp;
                interTimings.add(interDelta);
            }
        }

        return interTimings;
    }

    /** getSampleNGrams
     *
     * Converts a set of timings into a data structure that maps n-grams into lists of timings of the following form:
     *
     *   `[l, i, l, ..., l]`
     *
     * where `l` is a letter timing and `i` is an inter-letter timing.
     *
     * @param charList - the list of characters (letters) in the sample
     * @param n - the number of grams to grammify the thing into
     */
    public Map<String,ArrayList<Double>> getSampleNGrams(ArrayList<String> charList,
                                                          ArrayList<Long> charTimings,
                                                          ArrayList<Long> interTimings, Integer n) {
        Map<String,ArrayList<Double>> nGrams = new HashMap<String,ArrayList<Double>>();
        Map<String,Long> nGramCounts = new HashMap<String,Long>();
        for (int i = 0; i < charList.size() - n; i++) {
            // The i-th n-gram string
            String nGramString = "";
            // The timings array for the i-th n-gram
            ArrayList<Double> nGramTimings = new ArrayList<Double>();
            for (int j = i; j < i + n; j++) {
                // Add the j-th character of the i-th n-gram to the string
                nGramString += charList.get(j);
                // Add the j-th character timing
                nGramTimings.add(charTimings.get(j).doubleValue());
                // Add the (j-1)-th inter-character timing
                if (j > i) {
                    nGramTimings.add(interTimings.get(j-1).doubleValue());
                }
            }
            // Note that we have seen this n-gram string
            if (! nGramCounts.containsKey(nGramString)) {
                nGramCounts.put(nGramString, ((Integer) 1).longValue());
            } else {
                nGramCounts.put(nGramString, nGramCounts.get(nGramString) + 1);
            }
            // Integrate the new data into the existing data
            if (nGramCounts.get(nGramString) == 1) {
                nGrams.put(nGramString, nGramTimings);
            } else {
                // Need to update averages
                for (int j = 0; j < n; j++) {
                    Double k = nGramCounts.get(nGramString).doubleValue();
                    Double newValue = nGramTimings.get(j);
                    Double oldValue = nGrams.get(nGramString).get(j);
                    nGrams.get(nGramString).set(j, (oldValue * (k - 1.0) / k) + (newValue * 1.0 / k));
                }
            }
        }
        return nGrams;
    }

    public Map<String,ArrayList<Double>> getSampleBiGrams(ArrayList<String> charList,
                                                         ArrayList<Long> charTimings,
                                                         ArrayList<Long> interTimings) {
        return this.getSampleNGrams(charList, charTimings, interTimings, 2);
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

        ArrayList<Map> dataList = typeData.getData();

        ArrayList<String> charList = this.getSampleCharList(dataList);
        ArrayList<Long> charTimings = this.getSampleCharTimings(dataList);
        ArrayList<Long> interTimings = this.getSampleInterTimings(dataList);

        this.insertSample(user, charList, charTimings, interTimings);

        return Response.ok().build();
    }
}
