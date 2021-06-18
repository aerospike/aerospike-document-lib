# Aerospike Document API
[![Build project](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml/badge.svg)](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/)

This project provides an API which allows Aerospike [CDT (Collection Data Type)](https://www.aerospike.com/docs/client/java/index.html) objects to be accessed and mutated using JSON like syntax. Effectively this provides what can be termed a document API as CDT objects can be used to represent JSON in the Aerospike database.

*This project is now in beta. If you’re an enterprise customer feel free to reach out to our support with feedback and feature requests. We appreciate feedback from the Aerospike community on [issues](https://github.com/aerospike/aerospike-document-lib/issues) related to the Document API library.*

**Assumptions :** 

Familiarity with Aerospike - see [Java Introduction](https://www.aerospike.com/docs/client/java/index.html) if needed.

Some knowledge of Aerospike CDTs - see reference above, but not essential

## Build instructions
```sh
mvn clean package
```

## Quick Start

Consider the following json

``` json
{
  "forenames": [
    "Tommy",
    "Lee"
  ],
  "surname": "Jones",
  "date_of_birth": {
    "day": 15,
    "month": 9,
    "year": 1946
  },
  "selected_filmography":{
    "2012":["Lincoln","Men In Black 3"],
    "2007":["No Country For Old Men"],
    "2002":["Men in Black 2"],
    "1997":["Men in Black","Volcano"],
    "1994":["Natural Born Killers","Cobb"],
    "1991":["JFK"],
    "1980":["Coal Miner's Daughter","Barn Burning"]
  },
  "imdb_rank":{
    "source":"https://www.imdb.com/list/ls050274118/",
    "rank":51
  },
  "best_films_ranked": [
    {
      "source": "http://www.rottentomatoes.com",
      "films": ["The Fugitive","No Country For Old Men","Men In Black","Coal Miner's Daughter","Lincoln"]
    },
    {
      "source":"https://medium.com/the-greatest-films-according-to-me/10-greatest-films-of-tommy-lee-jones-97426103e3d6",
      "films":["The Three Burials of Melquiades Estrada","The Homesman","No Country for Old Men","In the Valley of Elah","Coal Miner's Daughter"]
    }
  ]
}
```

### Create

We add this to our Aerospike database as follows

``` java
   JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
   // For details of Aerospike namespace/set/key see https://www.aerospike.com/docs/architecture/data-model.html
   Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE,AEROSPIKE_SET,"tommy-lee-jones.json");
   documentClient.put(tommyLeeJonesDBKey, jsonNode);
```

### Get

We can find out the name of Jones' best film according to 'Rotten Tomatoes' using the path ```$.best_films_ranked[0].films[0]```

```java
   documentClient.get(tommyLeeJonesDBKey,"$.best_films_ranked[0].films[0]");
```

### Insert

We can add filmography for 2019 using the path ```$.selected_filmography.2019```

```java
  List<String> _2019Films = new Vector<String>();
  _2019Films.add("Ad Astra");
  documentClient.put(tommyLeeJonesDBKey,"$.selected_filmography.2019",_2019Films);
```

### Update

Update Jones' IMDB ranking using ```$.imdb_rank.rank```

``` java
  documentClient.put(tommyLeeJonesDBKey,"$.imdb_rank.rank",45);
```

### Append

We can append to 'Rotten Tomatoes' list of best films using the reference ```$.best_films_ranked[0].films```

```java
   documentClient.append(tommyLeeJonesDBKey,"$.best_films_ranked[0].films","Rolling Thunder");
   documentClient.append(tommyLeeJonesDBKey,"$.best_films_ranked[0].films","The Three Burials");
```

### Delete

We can delete a node e.g. the Medium reviewer's rankings

```java
   documentClient.delete(tommyLeeJonesDBKey,"$.best_films_ranked[1]");
```

### Document API

Below is the interface against which the API has been written

``` java
   /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Policy readPolicy, Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonObject  A JSON object to put.
     */
    void put(Key documentKey, JsonNode jsonObject);

    /**
     * Put a document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonObject  A JSON object to put.
     */
    void put(WritePolicy writePolicy, Key documentKey, JsonNode jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(WritePolicy writePolicy, Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;
```

The Aerospike Document Client is instantiated as follows

``` java
   AerospikeDocumentClient(IAerospikeClient client)
```

## References

See [AerospikeDocumentClient.java](../../../master/ken-tune/aerospike-document-api/src/main/java/com/aerospike/documentAPI/AerospikeDocumentClient.java) for full details of the API

See [AerospikeDocumentClientTest.java](../../../master/ken-tune/aerospike-document-api/src/test/java/com/aerospike/documentAPI/DocumentAPITest.java) for unit tests showing API usage
