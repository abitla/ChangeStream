package com.anant.changestreamapp;
import java.util.ArrayList;

import org.apache.commons.codec.digest.DigestUtils;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
public class ChangeReader {
    int numThreads;
    String sourceDB;
    String streamDB;
    String sourceCollection;
    String streamCollection;
    String sourceConn;
    String streamConn;
    int bsize;
    BsonDocument resumeToken;
  public ChangeReader( String sourceDB,String streamDB,String sourceCollection,String streamCollection,String sourceConn,String streamConn,int numThreads,int bsize,BsonDocument resumeToken) {
		// TODO Auto-generated constructor stub
	    this.numThreads=numThreads;
	    this.sourceDB=sourceDB;
	    this.streamDB=streamDB;
	    this.sourceCollection=sourceCollection;
	    this.streamCollection=streamCollection;
	    this.sourceConn=sourceConn;
	    this.streamConn=streamConn;
	    this.bsize=bsize;
	    this.resumeToken=resumeToken;
	}

	public void startReader() throws Exception{
		 MongoCursor<ChangeStreamDocument<Document>> cursor;
		 MongoClient mongoClientSrc=new MongoClient(sourceConn);
		 MongoClient mongoClientStream=new MongoClient(streamConn);
		 MongoDatabase srcdatabase=mongoClientSrc.getDatabase(sourceDB);
		 MongoDatabase streamDatabase=mongoClientStream.getDatabase(streamDB);
		 MongoCollection<Document> srcColl = srcdatabase.getCollection(sourceCollection);
		 MongoCollection<Document> streamColl = streamDatabase.getCollection(streamCollection);
		 if(resumeToken == null)
		 {	 
		cursor = srcColl.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
		 }
		 else{
		cursor = srcColl.watch().resumeAfter(resumeToken).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
				
		 }
		 ArrayList<WriteModel<Document>> bulkWriter=new ArrayList<>();
		 int count=1;
		 ChangeStreamDocument<Document> cStream;
		 try
		 {
		 while(cursor.hasNext()) {
			    
                cStream=cursor.next();
			    Document D=cStream.getFullDocument();
                //Generate an integer for a thread ensure that same _ids go to the same thread
			    
			    String md5Hex = DigestUtils
			    	      .md5Hex(D.getObjectId("_id").toHexString()).substring(0,8);
			    int randomNum=Math.abs((int) (Long.parseLong(md5Hex.toUpperCase(),16)%numThreads));
			    //int randomNum = rand.nextInt(numThreads);
			    Document insertDoc=new Document().append("resumeToken",cStream.getResumeToken()).append("thread",randomNum).append("payload",D).append("applied","N");
			    //Insert _id from ChanegSTream Doc to ensure uniqueness.
			    //System.out.println(insertDoc);
			    if(count < bsize){

			    	bulkWriter.add(new InsertOneModel<Document>(insertDoc));

			    	count++;
			    }
			    else{
			    	 count=1;
			    	 bulkWriter.add(new InsertOneModel<Document>(insertDoc));
			    	 bulkWriteFunction(streamColl,bulkWriter,new BulkWriteOptions().ordered(true));
			    	 bulkWriter.clear();
			
			    }
			 
		   
			}
       		 }
		 catch(Exception e){
			 if(bulkWriter.size()!=0){
				 bulkWriteFunction(streamColl,bulkWriter,new BulkWriteOptions().ordered(true));
			

			 }
			 System.out.println(e.getMessage());

			 
		 }
		 }
	

	
	  void bulkWriteFunction(MongoCollection<Document> targetCollection,ArrayList<WriteModel<Document>> bulkWriter,BulkWriteOptions bwOpt) throws Exception{
		    try
						{
		    	        //System.out.println(bulkWriter.toString());
					    BulkWriteResult bulkWriteResult=targetCollection.bulkWrite(bulkWriter,bwOpt);

					   
						}
					    catch (Exception e){
					    	//Add exception handling code here
					    	String error = e.getMessage();
					    	System.out.print(error);
					    	throw new Exception();

					    }


	   }
	public int getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	}
