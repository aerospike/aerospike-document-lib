# Aerospike Document API
[![Build project](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml/badge.svg)](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/)

This project provides an API which allows Aerospike [CDT (Collection Data Type)](https://www.aerospike.com/docs/client/java/index.html) objects to be accessed and mutated using JSON like syntax. Effectively this provides what can be termed a document API as CDT objects can be used to represent JSON in the Aerospike database.

**Assumptions :** 

Familiarity with Aerospike - see [Java Introduction](https://www.aerospike.com/docs/client/java/index.html) if needed.

Some knowledge of Aerospike CDTs - see reference above, but not essential

## Build instructions
```sh
mvn clean package
```

## Maven dependency

Add the Maven dependency:

```xml
<dependency>
  <groupId>com.aerospike</groupId>
  <artifactId>aerospike-document-api</artifactId>
  <version>1.0.0</version>
</dependency>
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

### Creating an Aerospike Document Client

The Aerospike Document Client is instantiated as follows
* You can create a new AerospikeClient using other constructors - in this example we are using IP and Port only.

``` java
   AerospikeClient client = new AerospikeClient(AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
   AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
```

### Create

We add this to our Aerospike database as follows

``` java
   JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
   // For details of Aerospike namespace/set/key see https://www.aerospike.com/docs/architecture/data-model.html
   Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "tommy-lee-jones.json");
   String documentBinName = "documentBin";
   documentClient.put(tommyLeeJonesDBKey, documentBinName, jsonNode);
```

### Get

We can find out the name of Jones' best film according to 'Rotten Tomatoes' using the path ```$.best_films_ranked[0].films[0]```

```java
   documentClient.get(tommyLeeJonesDBKey, documentBinName, "$.best_films_ranked[0].films[0]");
```

### Insert

We can add filmography for 2019 using the path ```$.selected_filmography.2019```

```java
  List<String> _2019Films = new Vector<String>();
  _2019Films.add("Ad Astra");
  documentClient.put(tommyLeeJonesDBKey, documentBinName, "$.selected_filmography.2019",_2019Films);
```

### Update

Update Jones' IMDB ranking using ```$.imdb_rank.rank```

``` java
  documentClient.put(tommyLeeJonesDBKey, documentBinName, "$.imdb_rank.rank",45);
```

### Append

We can append to 'Rotten Tomatoes' list of best films using the reference ```$.best_films_ranked[0].films```

```java
   documentClient.append(tommyLeeJonesDBKey, documentBinName, "$.best_films_ranked[0].films","Rolling Thunder");
   documentClient.append(tommyLeeJonesDBKey, documentBinName, "$.best_films_ranked[0].films","The Three Burials");
```

### Delete

We can delete a node e.g. the Medium reviewer's rankings

```java
   documentClient.delete(tommyLeeJonesDBKey, documentBinName, "$.best_films_ranked[1]");
```

### JsonPath Queries

JsonPath is a query language for JSON.
It supports operators, functions and filters.

Consider the following json

``` json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95,
        "ref": [1,2]
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99,
        "ref": [2,4,16]
      },
      {
        "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99,
        "ref": [1,3,5]
      },
      {
        "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99,
        "ref": [1,2,7]
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

Here are some examples:

```java
// All things, both books and bicycles
String jsonPath = "$.store.*";
Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// The authors of all books
String jsonPath = "$.store.book[*].author";
Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// The authors of all books
String jsonPath = "$.store.book[*].author";
String jsonObject = "J.K. Rowling";
// Modify the authors of all books to "J.K. Rowling"
documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, jsonObject);
Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// All books with an ISBN number
jsonPath = "$..book[?(@.isbn)]";
objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// All books in store cheaper than 10
jsonPath = "$.store.book[?(@.price < 10)]";
objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// All books matching regex (ignore case)
jsonPath = "$..book[?(@.author =~ /.*REES/i)]";
objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

// The price of everything
String jsonPath = "$.store..price";
// Delete the price field of every object exists in the store
documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);        
```

## References

See [AerospikeDocumentClient.java](../../../master/ken-tune/aerospike-document-api/src/main/java/com/aerospike/documentAPI/AerospikeDocumentClient.java) for full details of the API

See [AerospikeDocumentClientTest.java](../../../master/ken-tune/aerospike-document-api/src/test/java/com/aerospike/documentAPI/DocumentAPITest.java) for unit tests showing API usage
