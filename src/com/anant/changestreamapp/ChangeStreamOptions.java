package com.anant.changestreamapp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.BsonDocument;
import org.bson.Document;

public class ChangeStreamOptions {
	String sourceDB = "source";
	String sourceCollection = "source";
	String streamDB = "changes";
	String streamCollection = "changes";
	String targetDB = "target";
	String targetCollection = "target";
	String sourceConn = "localhost";
	String streamConn = "localhost";
	String targetConn = "localhost";
	String type="writer";
	int bsize=2500;
	BsonDocument resumeToken=null;
	
	int numThreads=16;
	boolean helpOnly=false;
	
	public ChangeStreamOptions(String[] args) throws ParseException {
		// TODO Auto-generated constructor stub
		Options cliopt;
		cliopt = new Options();
		CommandLineParser parser = new DefaultParser();
		cliopt.addOption("srcDB","SourceDatabase",true,"Source Database");
		cliopt.addOption("srcColl","SourceCollection",true,"Source Collection");
		cliopt.addOption("streamDB","StreamDatabase",true,"Stream Database");
		cliopt.addOption("streamColl","StreamCollection",true,"Stream Collection");
		cliopt.addOption("targetDB","TargetDatabase",true,"Target Database");
		cliopt.addOption("targetColl","TargetCollection",true,"Target Collection");
		cliopt.addOption("srcConn","SourceConnection",true,"Source Connection");
		cliopt.addOption("streamConn","StreamConnection",true,"Stream Connection");
		cliopt.addOption("targetConn","TargetConnection",true,"Target Connection");
		cliopt.addOption("nThreads","NumberofThreads",true,"Number of Threads");
		cliopt.addOption("type","ReaderorWriter",true,"Is it a reader or writer?");
		cliopt.addOption("bsize","Bufferbatchsize",true,"Buffer Batch Size");
		cliopt.addOption("token","ResumeToken",true,"Resume Token");
		cliopt.addOption("h","help",false,"Show Help");
		CommandLine cmd = parser.parse(cliopt, args);
		if(cmd.hasOption("srcDB"))
		{
			sourceDB = cmd.getOptionValue("srcDB");
		}
		
		if(cmd.hasOption("streamDB"))
		{
			streamDB = cmd.getOptionValue("streamDB");
		}
		if(cmd.hasOption("targetDb"))
		{
			streamDB = cmd.getOptionValue("targetDB");
		}
		if(cmd.hasOption("srcColl"))
		{
			sourceCollection = cmd.getOptionValue("srcColl");
		}
		
		if(cmd.hasOption("streamColl"))
		{
			streamCollection = cmd.getOptionValue("streamColl");
		}
		if(cmd.hasOption("targetColl"))
		{
			targetCollection = cmd.getOptionValue("targetColl");
		}
		
		if(cmd.hasOption("srcConn"))
		{
			sourceConn = cmd.getOptionValue("srcConn");
		}
		
		if(cmd.hasOption("streamConn"))
		{
			streamConn= cmd.getOptionValue("streamConn");
		}
		if(cmd.hasOption("targetConn"))
		{
			targetConn = cmd.getOptionValue("targetConn");
		}
		if(cmd.hasOption("nThreads"))
		{
			numThreads = Integer.parseInt(cmd.getOptionValue("nThreads"));
		}
			
		if(cmd.hasOption("type"))
		{
			type =cmd.getOptionValue("type");
		}
		if(cmd.hasOption("bsize"))
		{
			bsize = Integer.parseInt(cmd.getOptionValue("bsize"));
		}	
		if(cmd.hasOption("token"))
		{
			resumeToken =BsonDocument.parse(cmd.getOptionValue("token"));
		}	
		if(cmd.hasOption("h"))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "POCDriver", cliopt );
			helpOnly = true;
		}
	}

}
