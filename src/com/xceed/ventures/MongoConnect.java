package com.xceed.ventures;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoConnect {

	MongoClient mongoClient;
	DB db;

	/**
	 * Inserting tweets to database
	 */
	protected void Save(List jsonTweet,String database,String collection) {
		try {
			BasicDBObject dbObj=null; 
			Mongo m = new Mongo();
			DB db = m.getDB(database);
			DBCollection coll = db.getCollection(collection);
			
			for(Object obj : jsonTweet){
				dbObj = (BasicDBObject) JSON.parse((String)obj);
				coll.insert((DBObject) dbObj);
			}
			
			DBCursor myCursor = coll.find();
			int i=0;
			while(myCursor.hasNext()){
				System.out.println(i++ +". " + "Stored Tweets " + myCursor.next());
			}
			

			m.close();
		} catch (Exception e) {
			System.out.println("Exception " + e);
		}
	}

	
	/**
	 * Reading tweets from database	
	 */
	protected void Read(String database,String collection) {
		try {
			Mongo m = new Mongo();
			DB db = m.getDB(database);
			DBCollection coll = db.getCollection(collection);

			DBCursor myCursor = coll.find();
			int i = 0;
			while (myCursor.hasNext()) {
				System.out.println(i++ + ". " + "Stored Tweets "
						+ myCursor.next());
			}

			m.close();
		} catch (Exception e) {
			System.out.println("Exception " + e);
		}
	}
	
	/**
	 * Aggregate framework approach to fetch max count of a given field
	 * @param database
	 * @param collection
	 */
	protected void aggregate(String database,String collection){
		Mongo m = null;
		try {
			m = new Mongo();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DB db = m.getDB("twitter");
		DBCollection coll = db.getCollection("tweets1");
		
	
		// build the $projection operation
		DBObject fields = new BasicDBObject("screen_name", 1);
		fields.put("retweet_count", 1);
		//fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields );
		
		// Now the $group operation
		DBObject groupFields = new BasicDBObject("_id","screen_name");
		groupFields.put("maximum", new BasicDBObject( "$max", "$retweet_count"));
		DBObject group = new BasicDBObject("$group", groupFields);

		
		// run aggregation
		AggregationOutput output = coll.aggregate( group );
		
		System.out.println(output.getCommandResult());
	}
	
	/**
	 * Method to calculate Min, Max, Sum, Count, Avg, and Std deviation
	 */
	protected void mapReduce(String database,String collection,String fieldName){
		Mongo m = null;
		try {
			m = new Mongo();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DB db = m.getDB(database);
		DBCollection coll = db.getCollection(collection);
		
		String map="function() {"+
				"emit(1,"+
				"{sum: this."+fieldName+","+
				"min: this."+fieldName+","+
				"max: this."+fieldName+","+
				"count:1,"+
				"diff: 0,"+
				"});}";
		
		String reduce="function(key, values) {"+
						"var a = values[0]; "+
						"for (var i=1; i < values.length; i++){"+
						"var b = values[i]; "+
						"var delta = a.sum/a.count - b.sum/b.count; "+
						"var weight = (a.count * b.count)/(a.count + b.count);"+
						"a.diff += b.diff + delta*delta*weight;"+
						"a.sum += b.sum;"+
						"a.count += b.count;"+
						"a.min = Math.min(a.min, b.min);"+
						"a.max = Math.max(a.max, b.max);"+
						"}"+
						"return a;"+
						"}";
		
		String finalize="function(key, value){"+
						"value.avg = value.sum / value.count;"+
						"value.variance = value.diff / value.count;"+
						"value.stddev = Math.sqrt(value.variance);"+
						"return value;"+
						"}";
	
		
		 MapReduceCommand cmd = new MapReduceCommand(coll, map, reduce,
				     "finalize:"+finalize, MapReduceCommand.OutputType.INLINE, null);
				 
		 MapReduceOutput out = coll.mapReduce(cmd);
		 
		 for (DBObject o : out.results()) {
			 System.out.println(o.toString());
		 }
		
	
	}
}


	
