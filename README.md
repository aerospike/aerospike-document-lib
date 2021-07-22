# Aerospike Document API
[![Build project](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml/badge.svg)](https://github.com/aerospike/aerospike-document-lib/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-document-api/)

This project provides an API for accessing and mutating Aerospike
[Collection Data Type](https://www.aerospike.com/docs/client/java/index.html)(CDT)
objects using a [JSONPath](https://goessner.net/articles/JsonPath/) syntax.
This effectively provides a document API, with CDT objects used to represent
JSON documents in the Aerospike database.

### Assumptions

 * Familiarity with the Aerospike client for Java (see [Introduction - Java Client](https://www.aerospike.com/docs/client/java/index.html))
 * Some knowledge of Aerospike CDTs (see reference above)

## Getting Started Blog Posts

 1. [Aerospike Document API](https://medium.com/aerospike-developer-blog/aerospike-document-api-fd8870b4106c?source=friends_link&sk=b733e9fbe5a089ccca4f692e4f429711)
 2. [Aerospike Document API: JSONPath Queries](https://medium.com/aerospike-developer-blog/aerospike-document-api-jsonpath-queries-bd6260b2d076?source=friends_link&sk=d2c75b3beec691a36aa73513945f22a1)

## Build instructions
```sh
mvn clean package
```

### Maven dependency

Add the Maven dependency:

```xml
<dependency>
  <groupId>com.aerospike</groupId>
  <artifactId>aerospike-document-api</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Overview

Consider the following JSON:

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

### Instantiating an Aerospike Document Client

The Aerospike Document Client is instantiated as follows
* You can create a new AerospikeClient using other constructors - in this example we are using IP and Port only.

``` java
   AerospikeClient client = new AerospikeClient(AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
   AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
```

### Create

We add the example JSON document to our Aerospike database as follows

``` java
   JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
   // For details of Aerospike namespace/set/key see https://www.aerospike.com/docs/architecture/data-model.html
   Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "tommy-lee-jones.json");
   String documentBinName = "documentBin";
   documentClient.put(tommyLeeJonesDBKey, documentBinName, jsonNode);
```

### Insert

We can add filmography for 2019 using the JSONPath ```$.selected_filmography.2019```

```java
  List<String> _2019Films = new Vector<String>();
  _2019Films.add("Ad Astra");
  documentClient.put(tommyLeeJonesDBKey, documentBinName, "$.selected_filmography.2019",_2019Films);
```

### Update

Update Jones' IMDB ranking using the JSONPath ```$.imdb_rank.rank```

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

### Get

We can find out the name of Jones' best film according to 'Rotten Tomatoes' using the JSONPath ```$.best_films_ranked[0].films[0]```

```java
   documentClient.get(tommyLeeJonesDBKey, documentBinName, "$.best_films_ranked[0].films[0]");
```

## JSONPath Queries

JSONPath is a query language for JSON.
It supports operators, functions and filters.

Consider the following JSON document

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

#### Examples
Here are some examples of JSONPath queries:

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

## Multiple document bins

Starting at version `1.1.0` there is a new feature called multiple document bins.

You can have multiple documents - each stored in a different bin, all documents have the same structure but not the same data.

Example of a use-case can be storing events, each document contains events for a specific amount of time - for example, a week, and now you 
have the ability to use Document API operations (including JSONPath queries) on multiple documents (with the same structure) at once
using a single Aerospike operate() command under the hood which saves server resources boilerplate code.

### How it looks

Consider the following JSON documents:

events1.json
```json
{
  "authentication": {
    "login": [
      {
        "id": 1,
        "name": "John Smith",
        "location": "US",
        "date": "1.7.2021",
        "device": "Computer",
        "os": "Windows"
      },
      {
        "id": 2,
        "name": "Jonathan Sidwell",
        "location": "Israel",
        "date": "1.7.2021",
        "device": "Mobile",
        "os": "Android"
      },
      {
        "id": 3,
        "name": "Mike Ross",
        "location": "US",
        "date": "1.7.2021",
        "device": "Computer",
        "os": "MacOS"
      },
      {
        "id": 4,
        "name": "Jessica Pearson",
        "location": "France",
        "date": "1.7.2021",
        "device": "Computer",
        "os": "Windows"
      }
    ],
    "logout": {
      "name": "Nathan Levy",
      "datetime": "1.7.2021",
      "device": "Tablet",
      "ref": [7,4,2]
    }
  },
  "like": 10
}
```

events2.json
```json
{
  "authentication": {
    "login": [
      {
        "id": 21,
        "name": "Simba Lion",
        "location": "Italy",
        "date": "2.7.2021",
        "device": "Mobile",
        "os": "iOS"
      },
      {
        "id": 22,
        "name": "Sean Cahill",
        "location": "US",
        "date": "2.7.2021",
        "device": "Mobile",
        "os": "Android"
      },
      {
        "id": 23,
        "name": "Forrest Gump",
        "location": "Spain",
        "date": "2.7.2021",
        "device": "Computer",
        "os": "Windows"
      },
      {
        "id": 24,
        "name": "Patrick St. Claire",
        "location": "France",
        "date": "2.7.2021",
        "device": "Mobile",
        "os": "iOS"
      }
    ],
    "logout": {
      "name": "John Snow",
      "datetime": "2.7.2021",
      "device": "Mobile",
      "ref": [1,2,3]
    }
  },
  "like": 20
}
```

We have 2 documents with the same structure but not the same data that represents events.

Defining a bins list.
```java
String documentBinName1 = "events1Bin";
String documentBinName2 = "events2Bin";
List<String> bins = new ArrayList<>();
bins.add(documentBinName1);
bins.add(documentBinName2);
```

Examples:
```java
// The names of the users of all logout events from each document
String jsonPath = "$.authentication.logout.name";
Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

// Modify the devices of all the authentications (login and logout) to "Mobile"
jsonPath = "$.authentication..device";
jsonObject = "Mobile";
documentClient.put(TEST_AEROSPIKE_KEY, bins, jsonPath, jsonObject);

// Delete the user field from all of the authentications (login and logout)
jsonPath = "$.authentication..user";
documentClient.delete(TEST_AEROSPIKE_KEY, bins, jsonPath);

// All the logins with "id" greater than 10
jsonPath = "$.authentication.login[?(@.id > 10)]";
objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);
```

## References

 * See [AerospikeDocumentClient.java](../../../master/ken-tune/aerospike-document-api/src/main/java/com/aerospike/documentAPI/AerospikeDocumentClient.java) for full details of the API
 * See [AerospikeDocumentClientTest.java](../../../master/ken-tune/aerospike-document-api/src/test/java/com/aerospike/documentAPI/DocumentAPITest.java) for unit tests showing API usage

