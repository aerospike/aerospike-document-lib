# Aerospike Document API

This project provides an API which allows Aerospike [CDT (Collection Data Type)](https://www.aerospike.com/docs/client/java/index.html) objects to be accessed and mutated using JSON like syntax. Effectively this provides what can be termed a document API as CDT objects can be used to represent JSON in the Aerospike database.

**Assumptions :** 

Familiarity with Aerospike - see [Java Introduction](https://www.aerospike.com/docs/client/java/index.html) if needed.

Some knowledge of Aerospike CDTs - see reference above, but not essential

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
   Map jsonAsMap = AerospikeDocumentClient.jsonStringToMap(jsonString);
   // For details of Aerospike namespace/set/key see https://www.aerospike.com/docs/architecture/data-model.html
   Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE,AEROSPIKE_SET,"tommy-lee-jones.json");
   documentClient.put(tommyLeeJonesDBKey, jsonAsMap);
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
    * Retrieve the object in the document with key documentKey that is referenced by the Json path
    */
    Object get(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

   /**
    * Put a document
    */
    void put(Key documentKey, Map jsonObject);

   /**
    * Put a map representation of a JSON object at a particular path in a json document
    */
    void put(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

   /**
    * Append an object to a list in a document specified by a json path
    */
    void append(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

   /**
    * Delete an object in a document specified by a json path
    */
    void delete(Key documentKey, String jsonPath) 
      throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;
```

The Aerospike Document Client is instantiated as follows

``` java
   AerospikeDocumentClient(AerospikeClient client)
```

There are getters and setters for the read and write policies, as well as the default bin name

``` java
    public String getDocumentBinName() 
    public void setDocumentBinName(String documentBinName) {
    public Policy getReadPolicy() 
    public void setReadPolicy(Policy readPolicy)
    public WritePolicy getWritePolicy() 
    public void setWritePolicy(WritePolicy writePolicy)

```

Finally, this utility method is provided.

```java
public static Map jsonStringToMap(String jsonString)
```
## Build instructions

mvn clean compile assembly:single

## References

See [AerospikeDocumentClient.java](../../../master/ken-tune/aerospike-document-api/src/main/java/com/aerospike/documentAPI/AerospikeDocumentClient.java) for full details of the API

See [AerospikeDocumentClientTest.java](../../../master/ken-tune/aerospike-document-api/src/test/java/com/aerospike/documentAPI/DocumentAPITest.java) for unit tests showing API usage





