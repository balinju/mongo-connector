/**
 * Mule Mongo Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.mongo;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.apache.commons.lang.Validate;
import org.bson.types.BasicBSONList;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Transformer;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.module.mongo.api.IndexOrder;
import org.mule.module.mongo.api.MongoClient;
import org.mule.module.mongo.api.MongoClientAdaptor;
import org.mule.module.mongo.api.MongoClientImpl;
import org.mule.module.mongo.api.MongoCollection;
import org.mule.module.mongo.api.WriteConcern;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mule.module.mongo.api.DBObjects.adapt;
import static org.mule.module.mongo.api.DBObjects.from;

/**
 * MongoDB is an open source, high-performance, schema-free, document-oriented database that manages collections of
 * BSON documents.
 * 
 * @author MuleSoft, inc.
 */
@Connector(name = "mongo", schemaVersion = "2.0", friendlyName = "Mongo DB")
public class MongoCloudConnector
{

    private static final String CAPPED_DEFAULT_VALUE = "false";
    private static final String WRITE_CONCERN_DEFAULT_VALUE = "DATABASE_DEFAULT";

    /**
     * The host of the Mongo server
     */
    @Configurable
    @Optional
    @Default("localhost")
    private String host;
    /**
     * The port of the Mongo server
     */
    @Configurable
    @Optional
    @Default("27017")
    private int port;
    /**
     * The database name of the Mongo server
     */
    @Configurable
    @Optional
    @Default("test")
    private String database;

    private MongoClient client;

    /**
     * Lists names of collections available at this database
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:list-collections}
     * 
     * @return the list of names of collections available at this database
     */
    @Processor
    public Collection<String> listCollections()
    {
        return client.listCollections();
    }

    /**
     * Answers if a collection exists given its name
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:exists-collection}
     * 
     * @param collection the name of the collection
     * @return if the collection exists
     */
    @Processor
    public boolean existsCollection(String collection)
    {
        return client.existsCollection(collection);
    }

    /**
     * Deletes a collection and all the objects it contains. If the collection does
     * not exist, does nothing.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:drop-collection}
     * 
     * @param collection the name of the collection to drop
     */
    @Processor
    public void dropCollection(String collection)
    {
        client.dropCollection(collection);
    }

    /**
     * Creates a new collection. If the collection already exists, a MongoException
     * will be thrown.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:create-collection}
     * 
     * @param collection the name of the collection to create
     * @param capped if the collection will be capped
     * @param maxObjects the maximum number of documents the new collection is able
     *            to contain
     * @param size the maximum size of the new collection
     */
    @Processor
    public void createCollection(String collection,
                                 @Optional @Default(CAPPED_DEFAULT_VALUE) boolean capped,
                                 @Optional Integer maxObjects,
                                 @Optional Integer size)
    {
        client.createCollection(collection, capped, maxObjects, size);
    }

    /**
     * Inserts an object in a collection, setting its id if necessary.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:insert-object}
     *
     * @param collection the name of the collection where to insert the given object
     * @param dbObject a {@link DBObject} instance.
     * @param writeConcern the optional write concern of insertion
     * @return the id that was just insterted
     */
    @Processor
    public String insertObject(String collection,
                               DBObject dbObject,
                               @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        return client.insertObject(collection, dbObject, writeConcern);
    }

    /**
     * Inserts an object in a collection, setting its id if necessary.
     * <p/>
     * A shallow conversion into DBObject is performed - that is, no conversion is
     * performed to its values.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:insert-object-from-map}
     * 
     *
     * @param collection the name of the collection where to insert the given object
     * @param elementAttributes alternative way of specifying the element as a
     *            literal Map inside a Mule Flow
     * @param writeConcern the optional write concern of insertion
     * @return the id that was just insterted
     */
    @Processor
    public String insertObjectFromMap(String collection,
                                      @Placement(group = "Element Attributes") @Optional Map<String, String> elementAttributes,
                                      @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        return client.insertObject(collection, (DBObject) adapt(elementAttributes), writeConcern);
    }

    /**
     * Updates objects that matches the given query. If parameter multi is set to
     * false, only the first document matching it will be updated. Otherwise, all the
     * documents matching it will be updated.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:update-objects}
     * 
     * @param collection the name of the collection to update
     * @param query the {@link DBObject} query object used to detect the element to
     *            update.
     * @param element the {@link DBObject} mandatory object that will replace that
     *            one which matches the query.
     * @param upsert if the database should create the element if it does not exist
     * @param multi if all or just the first object matching the query will be
     *            updated
     * @param writeConcern the write concern used to update
     */
    @Processor
    public void updateObjects(String collection,
                              DBObject query,
                              DBObject element,
                              @Optional @Default(CAPPED_DEFAULT_VALUE) boolean upsert,
                              @Optional @Default("true") boolean multi,
                              @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.updateObjects(collection, query, element, upsert, multi, writeConcern);
    }

    /**
     * Updates objects that matches the given query. If parameter multi is set to
     * false, only the first document matching it will be updated. Otherwise, all the
     * documents matching it will be updated.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:update-objects-using-map}
     * 
     * @param collection the name of the collection to update
     * @param queryAttributes the query object used to detect the element to update.
     * @param elementAttributes the mandatory object that will replace that one which
     *            matches the query.
     * @param upsert if the database should create the element if it does not exist
     * @param multi if all or just the first object matching the query will be
     *            updated
     * @param writeConcern the write concern used to update
     */
    @Processor
    public void updateObjectsUsingMap(String collection,
                                      @Placement(group = "Query Attributes") Map<String, String> queryAttributes,
                                      @Placement(group = "Element Attributes") Map<String, String> elementAttributes,
                                      @Optional @Default(CAPPED_DEFAULT_VALUE) boolean upsert,
                                      @Optional @Default("true") boolean multi,
                                      @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.updateObjects(collection, (DBObject) adapt(queryAttributes), (DBObject) adapt(elementAttributes), upsert, multi,
            writeConcern);
    }

    /**
     * Inserts or updates an object based on its object _id.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:save-object}
     * 
     * @param collection the collection where to insert the object
     * @param element the mandatory {@link DBObject} object to insert.
     * @param writeConcern the write concern used to persist the object
     */
    @Processor
    public void saveObject(String collection,
                           DBObject element,
                           @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.saveObject(collection, from(element), writeConcern);
    }

    /**
     * Inserts or updates an object based on its object _id.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:save-object-from-map}
     * 
     * @param collection the collection where to insert the object
     * @param elementAttributes the mandatory object to insert.
     * @param writeConcern the write concern used to persist the object
     */
    @Processor
    public void saveObjectFromMap(String collection,
                                  @Placement(group = "Element Attributes") Map<String, String> elementAttributes,
                                  @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.saveObject(collection, (DBObject) adapt(elementAttributes), writeConcern);
    }

    /**
     * Removes all the objects that match the a given optional query. If query is not
     * specified, all objects are removed. However, please notice that this is
     * normally less performant that dropping the collection and creating it and its
     * indices again
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:remove-objects}
     * 
     * @param collection the collection whose elements will be removed
     * @param query the optional {@link DBObject} query object. Objects that match it
     *            will be removed.
     * @param writeConcern the write concern used to remove the object
     */
    @Processor
    public void removeObjects(String collection,
                              DBObject query,
                              @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.removeObjects(collection, query, writeConcern);
    }

    /**
     * Removes all the objects that match the a given optional query. If query is not
     * specified, all objects are removed. However, please notice that this is
     * normally less performant that dropping the collection and creating it and its
     * indices again
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:remove-using-query-map}
     * 
     * @param collection the collection whose elements will be removed
     * @param queryAttributes the query object. Objects that match it will be
     *            removed.
     * @param writeConcern the write concern used to remove the object
     */
    @Processor
    public void removeUsingQueryMap(String collection,
                                    @Placement(group = "Query Attributes") Map<String, String> queryAttributes,
                                    @Optional @Default(WRITE_CONCERN_DEFAULT_VALUE) WriteConcern writeConcern)
    {
        client.removeObjects(collection, (DBObject) adapt(queryAttributes), writeConcern);
    }

    /**
     * Transforms a collection into a collection of aggregated groups, by applying a
     * supplied element-mapping function to each element, that transforms each one
     * into a key-value pair, grouping the resulting pairs by key, and finally
     * reducing values in each group applying a suppling 'reduce' function.
     * <p/>
     * Each supplied function is coded in JavaScript.
     * <p/>
     * Note that the correct way of writing those functions may not be obvious;
     * please consult MongoDB documentation for writing them.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:map-reduce-objects}
     * 
     * @param collection the name of the collection to map and reduce
     * @param mapFunction a JavaScript encoded mapping function
     * @param reduceFunction a JavaScript encoded reducing function
     * @param outputCollection the name of the output collection to write the
     *            results, replacing previous collection if existed, mandatory when
     *            results may be larger than 16MB. If outputCollection is
     *            unspecified, the computation is performed in-memory and not
     *            persisted.
     * @return an iterable that retrieves the resulting collection of
     *         {@link DBObject}
     */
    @Processor
    public Iterable<DBObject> mapReduceObjects(String collection,
                                               String mapFunction,
                                               String reduceFunction,
                                               @Optional String outputCollection)
    {
        return client.mapReduceObjects(collection, mapFunction, reduceFunction, outputCollection);
    }

    /**
     * Counts the number of objects that match the given query. If no query is
     * passed, returns the number of elements in the collection
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:count-objects}
     * 
     * @param collection the target collection
     * @param query the optional {@link DBObject} query for counting objects. Only
     *            objects matching it will be counted. If unspecified, all objects
     *            are counted.
     * @return the amount of objects that matches the query
     */
    @Processor
    public long countObjects(String collection, @Optional DBObject query)
    {
        return client.countObjects(collection, query);
    }

    /**
     * Counts the number of objects that match the given query. If no query is
     * passed, returns the number of elements in the collection
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:count-objects-using-query-map}
     * 
     * @param collection the target collection
     * @param queryAttributes the optional query for counting objects. Only objects
     *            matching it will be counted. If unspecified, all objects are
     *            counted.
     * @return the amount of objects that matches the query
     */
    @Processor
    public long countObjectsUsingQueryMap(String collection, @Placement(group = "Query Attributes") @Optional Map<String, String> queryAttributes)
    {
        return client.countObjects(collection, (DBObject) adapt(queryAttributes));
    }

    /**
     * Finds all objects that match a given query. If no query is specified, all
     * objects of the collection are retrieved. If no fields object is specified, all
     * fields are retrieved.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:find-objects}
     * 
     * @param collection the target collection
     * @param query the optional {@link DBObject} query object. If unspecified, all
     *            documents are returned.
     * @param fields alternative way of passing fields as a literal List
     * @return an iterable of {@link DBObject}
     */
    @Processor
    public Iterable<DBObject> findObjects(String collection,
                                          @Optional DBObject query,
                                          @Placement(group = "Fields") @Optional List<String> fields)
    {
        return client.findObjects(collection, query, fields);
    }

    /**
     * Finds all objects that match a given query. If no query is specified, all
     * objects of the collection are retrieved. If no fields object is specified, all
     * fields are retrieved.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:find-objects-using-query-map}
     * 
     * @param collection the target collection
     * @param queryAttributes the optional query object. If unspecified, all
     *            documents are returned.
     * @param fields alternative way of passing fields as a literal List
     * @return an iterable of {@link DBObject}
     */
    @Processor
    public Iterable<DBObject> findObjectsUsingQueryMap(String collection,
                                                       @Placement(group = "Query Attributes") @Optional Map<String, String> queryAttributes,
                                                       @Placement(group = "Fields") @Optional List<String> fields)
    {
        return client.findObjects(collection, (DBObject) adapt(queryAttributes), fields);
    }

    /**
     * Finds the first object that matches a given query. Throws a
     * {@link MongoException} if no one matches the given query
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:find-one-object}
     * 
     * @param collection the target collection
     * @param query the mandatory {@link DBObject} query object that the returned
     *            object matches.
     * @param fields alternative way of passing fields as a literal List
     * @return a non-null {@link DBObject} that matches the query.
     */
    @Processor
    public DBObject findOneObject(String collection,
                                  DBObject query,
                                  @Placement(group = "Fields") @Optional List<String> fields)
    {
        return client.findOneObject(collection, query, fields);

    }

    /**
     * Finds the first object that matches a given query. Throws a
     * {@link MongoException} if no one matches the given query
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:find-one-object-using-query-map}
     * 
     * @param collection the target collection
     * @param queryAttributes the mandatory query object that the returned object
     *            matches.
     * @param fields alternative way of passing fields as a literal List
     * @return a non-null {@link DBObject} that matches the query.
     */
    @Processor
    public DBObject findOneObjectUsingQueryMap(String collection,
                                               @Placement(group = "Query Attributes") Map<String, String> queryAttributes,
                                               @Placement(group = "Fields") @Optional List<String> fields)
    {
        return client.findOneObject(collection, (DBObject) adapt(queryAttributes), fields);

    }

    /**
     * Creates a new index
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:create-index}
     * 
     * @param collection the name of the collection where the index will be created
     * @param field the name of the field which will be indexed
     * @param order the indexing order
     */
    @Processor
    public void createIndex(String collection, String field, @Optional @Default("ASC") IndexOrder order)
    {
        client.createIndex(collection, field, order);
    }

    /**
     * Drops an existing index
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:drop-index}
     * 
     * @param collection the name of the collection where the index is
     * @param index the name of the index to drop
     */
    @Processor
    public void dropIndex(String collection, String index)
    {
        client.dropIndex(collection, index);
    }

    /**
     * List existent indices in a collection
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:list-indices}
     * 
     * @param collection the name of the collection
     * @return a collection of {@link DBObject} with indices information
     */
    @Processor
    public Collection<DBObject> listIndices(String collection)
    {
        return client.listIndices(collection);
    }

    /**
     * Creates a new GridFSFile in the database, saving the given content, filename,
     * contentType, and extraData, and answers it.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:create-file-from-payload}
     * 
     * @param payload the mandatory content of the new gridfs file. It may be a
     *            java.io.File, a byte[] or an InputStream.
     * @param filename the mandatory name of new file.
     * @param contentType the optional content type of the new file
     * @param metadata the optional {@link DBObject} metadata of the new content type
     * @return the new GridFSFile {@link DBObject}
     * @throws IOException
     */
    @Processor
    public DBObject createFileFromPayload(@Payload Object payload,
                                          String filename,
                                          @Optional String contentType,
                                          @Optional DBObject metadata) throws IOException
    {
        InputStream stream = toStream(payload);
        try
        {
            return client.createFile(stream, filename, contentType, metadata);
        }
        finally
        {
            stream.close();
        }
    }

    private InputStream toStream(Object content) throws FileNotFoundException
    {
        if (content instanceof InputStream)
        {
            return (InputStream) content;
        }
        if (content instanceof byte[])
        {
            return new ByteArrayInputStream((byte[]) content);
        }
        if (content instanceof File)
        {
            return new FileInputStream((File) content);
        }
        throw new IllegalArgumentException("Content " + content + " is not supported");
    }

    /**
     * Lists all the files that match the given query
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:find-files}
     * 
     * @param query a {@link DBObject} query the optional query
     * @return a {@link DBObject} files iterable
     */
    @Processor
    public Iterable<DBObject> findFiles(@Optional DBObject query)
    {
        return client.findFiles(from(query));
    }

    /**
     * Lists all the files that match the given query
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:find-files-using-query-map}
     * 
     * @param queryAttributes the optional query attributes
     * @return a {@link DBObject} files iterable
     */
    @Processor
    public Iterable<DBObject> findFilesUsingQueryMap(@Placement(group = "Query Attributes") @Optional Map<String, String> queryAttributes)
    {
        return client.findFiles((DBObject) adapt(queryAttributes));
    }

    /**
     * Answers the first file that matches the given query. If no object matches it,
     * a MongoException is thrown.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:find-one-file}
     * 
     * @param query the {@link DBObject} mandatory query
     * @return a {@link DBObject}
     */
    @Processor
    public DBObject findOneFile(DBObject query)
    {
        return client.findOneFile(from(query));
    }

    /**
     * Answers the first file that matches the given query. If no object matches it,
     * a MongoException is thrown.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:find-one-file-using-query-map}
     * 
     * @param queryAttributes the mandatory query
     * @return a {@link DBObject}
     */
    @Processor
    public DBObject findOneFileUsingQueryMap(@Placement(group = "Query Attributes") Map<String, String> queryAttributes)
    {
        return client.findOneFile((DBObject) adapt(queryAttributes));
    }

    /**
     * Answers an inputstream to the contents of the first file that matches the
     * given query. If no object matches it, a MongoException is thrown.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:get-file-content}
     * 
     * @param query the {@link DBObject} mandatory query
     * @return an InputStream to the file contents
     */
    @Processor
    public InputStream getFileContent(DBObject query)
    {
        return client.getFileContent(from(query));
    }

    /**
     * Answers an inputstream to the contents of the first file that matches the
     * given queryAttributes. If no object matches it, a MongoException is thrown.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:get-file-content-using-query-map}
     * 
     * @param queryAttributes the mandatory query attributes
     * @return an InputStream to the file contents
     */
    @Processor
    public InputStream getFileContentUsingQueryMap(@Placement(group = "Query Attributes") Map<String, String> queryAttributes)
    {
        return client.getFileContent((DBObject) adapt(queryAttributes));
    }

    /**
     * Lists all the files that match the given query, sorting them by filename. If
     * no query is specified, all files are listed.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:list-files}
     * 
     * @param query the {@link DBObject} optional query
     * @return an iterable of {@link DBObject}
     */
    @Processor
    public Iterable<DBObject> listFiles(@Optional DBObject query)
    {
        return client.listFiles(from(query));
    }

    /**
     * Lists all the files that match the given query, sorting them by filename. If
     * no query is specified, all files are listed.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:list-files-using-query-map}
     * 
     * @param queryAttributes the optional query
     * @return an iterable of {@link DBObject}
     */
    @Processor
    public Iterable<DBObject> listFilesUsingQueryMap(@Placement(group = "Query Attributes") @Optional Map<String, String> queryAttributes)
    {
        return client.listFiles((DBObject) adapt(queryAttributes));
    }

    /**
     * Removes all the files that match the given query. If no query is specified,
     * all files are removed
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:remove-files}
     * 
     * @param query the {@link DBObject} optional query
     */
    @Processor
    public void removeFiles(@Optional DBObject query)
    {
        client.removeFiles(from(query));
    }

    /**
     * Removes all the files that match the given query. If no query is specified,
     * all files are removed
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:remove-files-using-query-map}
     * 
     * @param queryAttributes the optional query
     */
    @Processor
    public void removeFilesUsingQueryMap(@Placement(group = "Query Attributes") @Optional Map<String, String> queryAttributes)
    {
        client.removeFiles((DBObject) adapt(queryAttributes));
    }

    /**
     * Convert JSON to DBObject.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:jsonToDbobject}
     * 
     * @param input the input for this transformer
     * @return the converted {@link DBObject}
     */
    @Transformer(sourceTypes = {String.class})
    public static DBObject jsonToDbobject(Object input)
    {
        return (DBObject) JSON.parse((String) input);
    }

    /**
     * Convert DBObject to Json.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:dbobjectToJson}
     * 
     * @param input the input for this transformer
     * @return the converted string representation
     */
    @Transformer(sourceTypes = {DBObject.class})
    public static String dbobjectToJson(Object input)
    {
        return JSON.serialize(input);
    }

    /**
     * Convert a BasicBSONList into Json.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample mongo:bsonListToJson}
     * 
     * @param input the input for this transformer
     * @return the converted string representation
     */
    @Transformer(sourceTypes = {BasicBSONList.class})
    public static String bsonListToJson(Object input)
    {
        return JSON.serialize(input);
    }

    /**
     * Convert a BasicBSONList into Json.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:mongoCollectionToJson}
     * 
     * @param input the input for this transformer
     * @return the converted string representation
     */
    @Transformer(sourceTypes = {MongoCollection.class})
    public static String mongoCollectionToJson(Object input)
    {
        return JSON.serialize(input);
    }

    /**
     * Convert a DBObject into Map.
     * <p/>
     * {@sample.xml ../../../doc/mongo-connector.xml.sample
     * mongo:dbObjectToMap}
     *
     *
     * @param input the input for this transformer
     * @return the converted Map representation
     */
    @Transformer(sourceTypes = {DBObject.class})
    @SuppressWarnings("unchecked")
    public static Map dbObjectToMap(Object input)
    {
        return ((DBObject) input).toMap();
    }

    /**
     * Method invoked when a {@link MongoSession} needs to be created.
     * 
     * @param username the username to use in case authentication is required
     * @param password the password to use in case authentication is required, null
     *            if no authentication is desired
     * @return the newly created {@link MongoSession}
     * @throws org.mule.api.ConnectionException
     */
    @Connect
    public void connect(@ConnectionKey String username, @Password String password) throws ConnectionException
    {
        DB db = null;
        try
        {
            db = getDatabase(new Mongo(host, port), username, password);
        }
        catch (UnknownHostException e)
        {
            throw new ConnectionException(ConnectionExceptionCode.UNKNOWN_HOST, null, e.getMessage());
        }
        this.client = new MongoClientImpl(db);
    }

    /**
     * Method invoked when the {@link MongoSession} is to be destroyed.
     */
    @Disconnect
    public void disconnect()
    {
        this.client = null;
    }
    
    @ValidateConnection
    public boolean isConnected() {
        return this.client != null;
    }
    
    @ConnectionIdentifier
    public String connectionId() {
        return "unknown";
    }

    private DB getDatabase(Mongo mongo, String username, String password)
    {
        DB db = mongo.getDB(database);
        if (password != null)
        {
            Validate.notNull(username, "Username must not be null if password is set");
            db.authenticate(username, password.toCharArray());
        }
        return db;
    }

    protected MongoClient adaptClient(MongoClient client)
    {
        return MongoClientAdaptor.adapt(client);
    }

    public String getDatabase()
    {
        return database;
    }

    public void setDatabase(String database)
    {
        this.database = database;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }
}
