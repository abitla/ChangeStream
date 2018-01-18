package com.anant.changestreamapp;	
import java.sql.Time;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

public class ChangeWriter implements Runnable {
  int threadNum;
  MongoClient mongoClientStream;
  MongoClient mongoClientTarget;
  String streamDB;
  String streamCollection;
  String targetDB;
  String targetCollection;
  int bsize;

	public ChangeWriter(int num,MongoClient mongoClientStream, MongoClient mongoClientTarget, String streamDB, String streamCollection, String targetDB, String targetCollection,int bsize) {
		// TODO Auto-generated constructor stub
		threadNum=num;
		this.mongoClientStream=mongoClientStream;
		this.mongoClientTarget=mongoClientTarget;
		this.streamDB=streamDB;
		this.streamCollection=streamCollection;
		this.targetDB=targetDB;
		this.targetCollection=targetCollection;
		this.bsize=bsize;
		
	}
//create indexes on source collection threadNum+applied+_id


	public void run() {
		// TODO Auto-generated method stub
		 //MongoClient mongoClient=new MongoClient();
		 MongoDatabase sourceDatabase=mongoClientStream.getDatabase(streamDB);
		 MongoDatabase targetDatabase=mongoClientTarget.getDatabase(targetDB);

		 MongoCollection<Document> sourceColl = sourceDatabase.getCollection(streamCollection);
		 MongoCollection<Document> targetColl = targetDatabase.getCollection(targetCollection);
         while(true)
         {	 
		 MongoCursor<Document> cursor=sourceColl.find(Filters.and(Filters.eq("thread",threadNum),Filters.eq("applied","N"))).batchSize(2500).sort(new Document().append("_id", 1)).iterator();
		 int count=1;
		 ArrayList<WriteModel<Document>> bulkWriter=new ArrayList<>();
		 ArrayList<WriteModel<Document>> bulkUpdater=new ArrayList<>();
		 while(cursor.hasNext()) {
			    
			     Document D=cursor.next();



			    if(count < bsize){
			    	//System.out.println(D.get("_id"));

			    	bulkWriter.add(new ReplaceOneModel<Document>(Filters.eq("_id", D.get("payload",Document.class).getObjectId("_id")), D.get("payload",Document.class),new UpdateOptions().upsert(true) ));
			    	bulkUpdater.add(new UpdateOneModel<Document>(Filters.eq("_id", D.getObjectId("_id")),new Document("$set", new Document("applied", "Y"))));

			    	count++;
			    }
			 
			    else{
			
			    count=1;
			    try {
			    	bulkWriter.add(new ReplaceOneModel<Document>(Filters.eq("_id", D.getObjectId("_id")), D,new UpdateOptions().upsert(true) ));
			    	bulkUpdater.add(new UpdateOneModel<Document>(Filters.eq("_id", D.getObjectId("_id")),new Document("$set", new Document("applied", "Y"))));
					bulkWriteFunction(targetColl,sourceColl,bulkWriter,bulkUpdater);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    bulkWriter.clear();
			    bulkUpdater.clear();
			   

			}

		
		 }
		 if(bulkWriter.size()!=0){
			  try {
	bulkWriteFunction(targetColl,sourceColl,bulkWriter,bulkUpdater);
} catch (Exception e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
}
		 try {
			Thread.sleep(10000);
		

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

         }
    


	}
   static void bulkWriteFunction(MongoCollection<Document> targetCollection,MongoCollection<Document> sourceCollection,ArrayList<WriteModel<Document>> bulkWriter,ArrayList<WriteModel<Document>> bulkUpdater) throws Exception{
	    try
					{
				    BulkWriteResult bulkWriteResult1=targetCollection.bulkWrite(bulkWriter);
				    //System.out.println(bulkUpdater);
				    BulkWriteResult bulkWriteResult2=sourceCollection.bulkWrite(bulkUpdater);
				    //System.out.println("Done");
				    bulkWriter.clear();
				    bulkUpdater.clear();
					}
				    catch (Exception e){
				    	//Add exception handling code here
				    	String error = e.getMessage();
				    	System.out.print(error);
				    	//throw new Exception();

				    }


   }
}
