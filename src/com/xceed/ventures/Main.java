package com.xceed.ventures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;

import twitter4j.Twitter;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class Main {
    
	public static void main(String str[]){
		//Initialize OAuth and connect to Twitter API
		TweetOConnect tweetConnect = new TweetOConnect();
		
		//Read Tweets
		Twitter twitter = tweetConnect.connect();
		
		//Initialize DB
		MongoConnect mongo = new MongoConnect();
		
		//Save tweets in Database
		mongo.Save(tweetConnect.read(twitter,1,100),"twitter","tweets1");
		
		System.out.println("Fetching maximum retweet_count using agreegration");
		mongo.aggregate("twitter","tweets1");
		
		System.out.println("");
		//generate Min, Max, Sum, Count
		// field retweet_count
		System.out.println("Fetching Min, Max, Sum, Count using map reduce function");
		mongo.mapReduce("twitter","tweets1","retweet_count");
		
	}
    
	//simple test method to test connecting to twitter and storing in mongodb
    private void test(){
    	System.setProperty("java.net.useSystemProxies", "true");
        
        //Connecting to MongoDB
        Mongo m=null;
		try {
			m = new Mongo();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        DB db = m.getDB("twitter");
        DBCollection coll = db.getCollection("tweets");

        //Fetching tweets from Twitter
        String urlstr = "http://search.twitter.com/" +
                "search.json?q=fahadnajib&count=100";
        URL url=null;
		try {
			url = new URL(urlstr);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        URLConnection con=null;
		try {
			con = url.openConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        BufferedReader br=null;
		try {
			br = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int c;
        StringBuffer content = new StringBuffer();
        try {
			while((c=br.read())!=-1) {
			    content.append((char)c);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //System.out.println("Content "+content);
        System.out.println("Content String"+content.toString());
        
        //Inserting tweets to database        
        BasicDBObject res = (BasicDBObject)
                JSON.parse(content.toString());
        BasicDBList list;
        list = (BasicDBList)res.get("results");
        for(Object obj : list) {
            coll.insert((DBObject)obj);
        }
        
        DBObject myObj=coll.findOne();
        System.out.println("Stored Tweets "+myObj);
        
        m.close();
    }

}