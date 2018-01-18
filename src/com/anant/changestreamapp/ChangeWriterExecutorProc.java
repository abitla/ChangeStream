package com.anant.changestreamapp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.MongoClient;



public class ChangeWriterExecutorProc  {
  
	public ChangeWriterExecutorProc() {
		// TODO Auto-generated constructor stub
	}
    public void execute(String streamConn,String targetConn, String streamDB, String targetDB,String streamCollection,String targetCollection,int numThreads,int bsize){
	  MongoClient mongoClientStream=new MongoClient(streamConn);
	  MongoClient mongoClientTarget=new MongoClient(targetConn);
  ExecutorService testexec = Executors.newFixedThreadPool(numThreads);
  try {
      
  for (int i = 0; i < numThreads; i++) {
	 
      testexec.execute(new ChangeWriter(i,mongoClientStream,mongoClientTarget,streamDB,streamCollection,targetDB,targetCollection, bsize));
  }
  }
  catch(Exception e)
  {
  testexec.shutdown();
  }
  }
}

