package com.anant.changestreamapp;

import org.apache.commons.cli.*;

public class ChangeStreamApp {
    
	public ChangeStreamApp() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ChangeStreamOptions chOpts;
		
		try
		{
			
		chOpts=new ChangeStreamOptions(args);
	
		 if(chOpts.helpOnly){
			 return;
		 }
		}catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try{
		 if(chOpts.type.equals("reader")){
			 
		 
		 ChangeReader changeReader=new ChangeReader(chOpts.sourceDB,chOpts.streamDB,chOpts.sourceCollection,chOpts.streamCollection,chOpts.sourceConn,chOpts.streamConn,chOpts.numThreads,chOpts.bsize,chOpts.resumeToken);
		 changeReader.startReader();
		 }
		 else
		 {
		 ChangeWriterExecutorProc c=new ChangeWriterExecutorProc();
		 c.execute(chOpts.streamConn, chOpts.targetConn, chOpts.streamDB, chOpts.targetDB,chOpts.streamCollection,chOpts.targetCollection,chOpts.numThreads,chOpts.bsize);
		 }
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}


	}

}
