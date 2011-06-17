Mule Mongo Cloud Connector
==========================

Mule Cloud connector to mongo

Installation
------------

The connector can either be installed for all applications running within the Mule instance or can be setup to be used
for a single application.

*All Applications*

Download the connector from the link above and place the resulting jar file in
/lib/user directory of the Mule installation folder.

*Single Application*

To make the connector available only to single application then place it in the
lib directory of the application otherwise if using Maven to compile and deploy
your application the following can be done:

Add the connector's maven repo to your pom.xml:

    <repositories>
        <repository>
            <id>muleforge-releases</id>
            <name>MuleForge Snapshot Repository</name>
            <url>http://repository.mulesoft.org/releases/</url>
            <layout>default</layout>
        </repsitory>
    </repositories>

Add the connector as a dependency to your project. This can be done by adding
the following under the dependencies element in the pom.xml file of the
application:

    <dependency>
        <groupId>org.mule.modules</groupId>
        <artifactId>mule-module-mongo</artifactId>
        <version>1.0</version>
    </dependency>

Configuration
-------------

You can configure the connector as follows:

    <mongo:config client="value" database="value" host="value" port="value" password="value" username="value"/>

Here is detailed list of all the configuration attributes:

| attribute | description | optional | default value |
|:-----------|:-----------|:---------|:--------------|
|name|Give a name to this configuration so it can be later referenced by config-ref.|yes||
|client||yes|
|database|The database name of the Mongo server|yes|test
|host|The host of the Mongo server|yes|localhost
|port|The port of the Mongo server|yes|27017
|password|The user password. Only required for collections that require authentication|yes|
|username|The user name. Only required for collections that require authentication|yes|


List Collections
----------------

Lists names of collections available at this database



     <list-collections/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||

Returns list of names of collections available at this database



Exists Collection
-----------------

Answers if a collection exists given its name



     <exists-collection name="aColllection"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection|no||

Returns the collection exists



Drop Collection
---------------

Deletes a collection and all the objects it contains.  
If the collection does not exist, does nothing.



     <drop-collection name="aCollection"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection to drop|no||



Create Collection
-----------------

Creates a new collection. 
If the collection already exists, a MongoException will be thrown.



     <create-collection name="aCollection" capped="true"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection to create|no||
|capped|if the collection will be capped|yes|false|
|maxObjects|the maximum number of documents the new collection is able to contain|yes||
|size|the maximum size of the new collection|yes||



Insert Object
-------------

Inserts an object in a collection, setting its id if necessary.

Object can either be a raw DBObject, a String-Object Map or a JSon String.
If it is passed as Map, a shallow conversion into DBObject is performed - that is, no conversion is performed to its values.
If it is passed as JSon String, _ids of type ObjectId must be passed as a String, for example: 
{ "_id": "ObjectId(4df7b8e8663b85b105725d34)", "foo" : 5, "bar": [ 1 , 2 ] }



     <insert-object collection="Employees" object="#[header:aBsonEmployee]" writeConcern="SAFE"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection where to insert the given object|no||
|element|the object to insert. Maps, JSon Strings and DBObjects are supported.|yes||
|elementAttributes|alternative way of specifying the element as a literal Map inside a Mule Flow|yes||
|writeConcern|the optional write concern of insertion|yes|DATABASE_DEFAULT|*NONE*, *NORMAL*, *SAFE*, *FSYNC_SAFE*, *REPLICAS_SAFE*, *DATABASE_DEFAULT*, *mongoWriteConcern*



Update Objects
--------------

Updates objects that matches the given query. If parameter multi is set to false,
only the first document matching it will be updated. 
Otherwise, all the documents matching it will be updated.  



     <update-objects collection="#[map-payload:aCollectionName]" 
            query="#[variable:aBsonQuery]" object="#[variable:aBsonObject]" upsert="true"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection to update|no||
|query|the query object used to detect the element to update. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|queryAttributes|alternative way of passing query as a literal Map inside a Mule flow|yes||
|element|the mandatory object that will replace that one which matches the query. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|elementAttributes|alternative way of specifying the element as a literal Map inside a Mule Flow|yes||
|upsert|if the database should create the element if it does not exist|yes|false|
|multi|if all or just the first object matching the query will be updated|yes|true|
|writeConcern|the write concern used to update|yes|DATABASE_DEFAULT|*NONE*, *NORMAL*, *SAFE*, *FSYNC_SAFE*, *REPLICAS_SAFE*, *DATABASE_DEFAULT*, *mongoWriteConcern*



Save Object
-----------

Inserts or updates an object based on its object _id.
 


     <save-object 
             collection="#[map-payload:aCollectionName]"
             object="#[header:aBsonObject]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the collection where to insert the object|no||
|element|the mandatory object to insert. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|elementAttributes|an alternative way of passing the element as a literal Map inside a Mule Flow|yes||
|writeConcern|the write concern used to persist the object|yes|DATABASE_DEFAULT|*NONE*, *NORMAL*, *SAFE*, *FSYNC_SAFE*, *REPLICAS_SAFE*, *DATABASE_DEFAULT*, *mongoWriteConcern*



Remove Objects
--------------

Removes all the objects that match the a given optional query. 
If query is not specified, all objects are removed. However, please notice that this is normally
less performant that dropping the collection and creating it and its indices again



     <remove-objects collection="#[map-payload:aCollectionName]" query="#[map-payload:aBsonQuery]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the collection whose elements will be removed|no||
|query|the optional query object. Objects that match it will be removed. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|queryAttributes|an alternative way of passing the query as a literal Map inside a Mule Flow|yes||
|writeConcern|the write concern used to remove the object|yes|DATABASE_DEFAULT|*NONE*, *NORMAL*, *SAFE*, *FSYNC_SAFE*, *REPLICAS_SAFE*, *DATABASE_DEFAULT*, *mongoWriteConcern*



Map Reduce Objects
------------------

Transforms a collection into a collection of aggregated groups, by
applying a supplied element-mapping function to each element, that transforms each one
into a key-value pair, grouping the resulting pairs by key, and finally 
reducing values in each group applying a suppling 'reduce' function.   

Each supplied function is coded in JavaScript.

Note that the correct way of writing those functions may not be obvious; please 
consult MongoDB documentation for writing them.
 


      <map-reduce-objects 
         collection="myCollection"
         mapFunction="#[header:aJSMapFunction]"
         reduceFunction="#[header:aJSReduceFunction]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection to map and reduce|no||
|mapFunction|a JavaScript encoded mapping function|no||
|reduceFunction|a JavaScript encoded reducing function|no||
|outputCollection|the name of the output collection to write the results, replacing previous collection if existed, mandatory when results may be larger than 16MB. If outputCollection is unspecified, the computation is performed in-memory and not persisted.|yes||

Returns iterable that retrieves the resulting collection DBObjects



Count Objects
-------------

Counts the number of objects that match the given query. If no query
is passed, returns the number of elements in the collection



     <count-objects 
         collection="#[variable:aCollectionName]"
         query="#[variable:aBsonQuery]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the target collection|no||
|query|the optional query for counting objects. Only objects matching it will be counted. If unspecified, all objects are counted. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|queryAttributes|an alternative way of passing the query as a literal Map inside a Mule Flow|yes||

Returns amount of objects that matches the query



Find Objects
------------

Finds all objects that match a given query. If no query is specified, all objects of the 
collection are retrieved. If no fields object is specified, all fields are retrieved. 



     <find-objects query="#[map-payload:aBsonQuery]" fields-ref="#[header:aBsonFieldsSet]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the target collection|no||
|query|the optional query object. If unspecified, all documents are returned. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|queryAttributes|alternative way of passing the query object, as a literal Map inside a Mule Flow|yes||
|fieldsRef|an optional list of fields to return. If unspecified, all fields are returned.|yes||
|fields|alternative way of passing fields as a literal List|yes||

Returns iterable of DBObjects



Find One Object
---------------

Finds the first object that matches a given query. 
Throws a {@link MongoException} if no one matches the given query 



     <find-one-object  query="#[variable:aBsonQuery]" >
            <fields>
              <field>Field1</field>
              <field>Field2</field>
            </fields>
      </find-one-object>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the target collection|no||
|query|the mandatory query object that the returned object matches. Maps, JSon Strings and DBObjects are supported, as described in insert-object operation.|yes||
|queryAttributes|alternative way of passing the query object, as a literal Map inside a Mule Flow|yes||
|fieldsRef|an optional list of fields to return. If unspecified, all fields are returned.|yes||
|fields|alternative way of passing fields as a literal List|yes||

Returns non-null DBObject that matches the query.



Create Index
------------

Creates a new index



     <create-index collection="myCollection" keys="#[header:aBsonFieldsSet]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection where the index will be created|no||
|field|the name of the field which will be indexed|no||
|order|the indexing order|yes|ASC|*ASC*, *DESC*



Drop Index
----------

Drops an existing index
 


     <drop-index collection="myCollection" name="#[map-payload:anIndexName]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection where the index is|no||
|index|the name of the index to drop|no||



List Indices
------------

List existent indices in a collection



     <drop-index collection="myCollection" name="#[map-payload:anIndexName]"/>

| attribute | description | optional | default value | possible values |
|:-----------|:-----------|:---------|:--------------|:----------------|
|config-ref|Specify which configuration to use for this invocation|yes||
|collection|the name of the collection|no||

Returns collection of DBObjects with indices information

























































