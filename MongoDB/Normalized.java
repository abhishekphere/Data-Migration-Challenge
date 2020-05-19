import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Normalized {
		
	public static void main(String args[]) {
		
		Connection connect = null;
		try{  
			// MySQL
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "root";
			String password = "01973";
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			Statement statement = connect.createStatement();  

			
			
			// MongoDB
			MongoClient mongoClient = new MongoClient();
			
			// populating Mongo database 
			populateMongoDB(mongoClient, statement);
			
			queries(mongoClient, statement);
			
			System.out.println("Done"); 
			
			connect.close();   
		} 		
		catch (MongoException e) {
			e.printStackTrace();
		}
		catch(Exception e){  
			System.out.println(e);
		} 
		
	}
	public static void queries(MongoClient mongoClient, Statement statement) {
		
		DB database = mongoClient.getDB("IMDB"); 
		 	
		DBCollection movies = database.getCollection("movies");
		DBCollection directors = database.getCollection("directors");
		DBCollection genres = database.getCollection("genres");
		DBCollection actors = database.getCollection("actors");
		DBCollection roles = database.getCollection("roles");
		DBCollection directedby = database.getCollection("directedby");
		DBCollection moviegenres = database.getCollection("moviegenres");
		
		
		long t1, t2;
//		query1
		t1 = System.currentTimeMillis();
		
		AggregationOutput results1 = directors.aggregate(Arrays.asList(
				new BasicDBObject().append("$lookup", new BasicDBObject().append("from","directedby").append("localField","_id").append("foreignField", "directorid").append("as", "directordirectedby"))
				,new BasicDBObject().append("$match", new BasicDBObject().append("first", "James (I)").append("last","Cameron"))
				,new BasicDBObject().append("$unwind","$directordirectedby")
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","roles").append("localField","directordirectedby.movieid").append("foreignField", "movieid").append("as", "directedbyroles"))
				,new BasicDBObject().append("$unwind","$directedbyroles")
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","movies").append("localField","directedbyroles.movieid").append("foreignField", "_id").append("as", "rolesmovies"))
				,new BasicDBObject().append("$unwind","$rolesmovies")
				,new BasicDBObject().append("$match", new BasicDBObject().append("rolesmovies.year", new BasicDBObject().append("$gte" ,1989).append("$lte" ,1995)))
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","moviegenres").append("localField","rolesmovies._id").append("foreignField", "movieid").append("as", "moviegenredata"))
				,new BasicDBObject().append("$unwind","$moviegenredata")
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("localField","moviegenredata.genreid").append("from","genres").append("foreignField", "_id").append("as", "genredata"))
				,new BasicDBObject().append("$match", new BasicDBObject().append("genredata.genre", "Sci-Fi"))
				,new BasicDBObject().append("$group", new BasicDBObject().append("_id", null).append("count", new BasicDBObject().append("$sum", 1)))
				,new BasicDBObject().append("$project", new BasicDBObject().append("_id", 0).append("count", 1))
		));
		System.out.println("Query 1: ");
		System.out.println(results1.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
//		query2
		t1 = System.currentTimeMillis();
		AggregationOutput results2 =directors.aggregate(Arrays.asList(
				new BasicDBObject().append("$lookup", new BasicDBObject().append("from","directedby").append("localField","_id").append("foreignField", "directorid").append("as", "directordirectedby"))
				,new BasicDBObject().append("$match", new BasicDBObject().append("first", "Steven").append("last","Spielberg"))
				,new BasicDBObject().append("$unwind","$directordirectedby")
				,new BasicDBObject().append("$lookup", new  BasicDBObject().append("from","moviegenres").append("localField","directordirectedby.movieid").append("foreignField", "movieid").append("as", "moviegenredata"))
				,new BasicDBObject().append("$unwind","$moviegenredata")
				,new BasicDBObject().append("$lookup", new  BasicDBObject().append("from","movies").append("localField","moviegenredata.movieid").append("foreignField", "_id").append("as", "moviesdata"))
				,new BasicDBObject().append("$unwind","$moviesdata")
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","genres").append("localField","moviegenredata.genreid").append("foreignField", "_id").append("as", "genredata"))
				,new BasicDBObject().append("$unwind","$genredata")
				,new BasicDBObject().append("$match", new BasicDBObject().append("genredata.genre", "Drama"))
				,new BasicDBObject().append("$project", new BasicDBObject().append("moviesdata.title", 1))
		));   
		System.out.println("Query 2: ");
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		int count = 0;
		for (DBObject result : results2.results()) {
			count = count + 1;
			System.out.println(result);
		}
		System.out.println("Number of actors: "+count);
		
		
		
//		query 3
		t1 = System.currentTimeMillis();
		AggregationOutput results3 =directors.aggregate(Arrays.asList(
				new BasicDBObject().append("$lookup", new BasicDBObject().append("from","directedby").append("localField","_id").append("foreignField", "directorid").append("as", "directordirectedby"))
				,new BasicDBObject().append("$match", new BasicDBObject().append("first", "Steven").append("last","Spielberg"))
				,new BasicDBObject().append("$unwind","$directordirectedby")  
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","roles").append("localField","directordirectedby.movieid").append("foreignField", "movieid").append("as", "rolesdata"))
				,new BasicDBObject().append("$unwind","$rolesdata")
				,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","actors").append("localField","rolesdata.actorid").append("foreignField", "_id").append("as", "actordata"))			
				,new BasicDBObject().append("$match", new BasicDBObject().append("actordata.first", "Tom").append("actordata.last", "Hanks"))
				,new BasicDBObject().append("$group", new BasicDBObject().append("_id", null).append("count", new BasicDBObject().append("$sum", 1)))
				,new BasicDBObject().append("$project", new BasicDBObject().append("count", 1))	
		));
		System.out.println("Query 3: ");
		System.out.println(results3.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
		
//		query 4
		t1 = System.currentTimeMillis();
		AggregationOutput results4 =directors.aggregate(Arrays.asList(
			 new BasicDBObject().append("$lookup", new BasicDBObject().append("from","directedby").append("localField","_id").append("foreignField", "directorid").append("as", "directordirectedby"))
			,new BasicDBObject().append("$match", new BasicDBObject().append("first", "Woody").append("last","Allen"))
			,new BasicDBObject().append("$unwind","$directordirectedby")
			,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","moviegenres").append("localField","directordirectedby.movieid").append("foreignField", "movieid").append("as", "moviegenredata"))
			,new BasicDBObject().append("$unwind","$moviegenredata")
			,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","genres").append("localField","moviegenredata.genreid").append("foreignField", "_id").append("as", "genredata"))
			,new BasicDBObject().append("$match", new BasicDBObject().append("genredata.genre", "Comedy"))
			,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","roles").append("localField","directordirectedby.movieid").append("foreignField", "movieid").append("as", "rolesdata"))
			,new BasicDBObject().append("$unwind","$rolesdata")
			,new BasicDBObject().append("$lookup", new BasicDBObject().append("from","actors").append("localField","rolesdata.actorid").append("foreignField", "_id").append("as", "actordata"))
			,new BasicDBObject().append("$unwind","$actordata")	 
			,new BasicDBObject().append("$group", new BasicDBObject().append("_id", new BasicDBObject().append("first", "$actordata.first").append("last", "$actordata.last")).append("count", new BasicDBObject().append("$sum",1)))
			,new BasicDBObject().append("$match", new BasicDBObject().append("count", new BasicDBObject().append("$gt", 3)))
		));
	 	
		System.out.println("Query 4: ");
		count = 0; 
		for (DBObject result : results4.results()) {
			count = count + 1;
			System.out.println(result);
		}
		System.out.println("Number of actors: "+count);
		
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println(); 
		 

	}
	
	public static void populateMongoDB(MongoClient mongoClient, Statement statement) {
		// Creating DB
		DB database = mongoClient.getDB("IMDB");
		 
		// Creating collections
		DBCollection movies = database.getCollection("movies");
		DBCollection directors = database.getCollection("directors");
		DBCollection genres = database.getCollection("genres");
		DBCollection actors = database.getCollection("actors");
		DBCollection roles = database.getCollection("roles");
		DBCollection directedby = database.getCollection("directedby");
		DBCollection moviegenres = database.getCollection("moviegenres");
		
		String query = ""; 
		int id = 0;
		DBObject document = null;
		
		//directors
		query = "Select * from directors;";
		String dfirst = "", dlast = "";
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("id");
				dfirst = rs.getString("first");
				dlast = rs.getString("last");
				
				document = new BasicDBObject("_id", id)
                        .append("first", dfirst)
                        .append("last", dlast);

				directors.insert(document); 
								
			}
			
			System.out.println("directors done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//actors
		query = "Select * from actors;";
		String afirst = "", alast = "";
		String gender = "";
		
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("id");
				afirst = rs.getString("first");
				alast = rs.getString("last");
				gender = rs.getString("gender");
				
				document = new BasicDBObject("_id", id)
                        .append("first", afirst)
                        .append("last", alast)
                        .append("gender", gender);

				actors.insert(document);  
				
			}
			System.out.println("actors done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//movies
		query = "Select * from movies;";
		String title = "";
		int year = 0;
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("id");
				title = rs.getString("title");
				year = rs.getInt("year");
				
				document = new BasicDBObject("_id", id)
                        .append("title", title)
                        .append("year", year);

				movies.insert(document); 
				
			}
			System.out.println("movies done");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//Genres
		query = "Select * from genres;";
		String genre = "";
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("id");
				genre = rs.getString("genre");
				
				document = new BasicDBObject("_id", id)
                        .append("genre", genre);

				genres.insert(document); 
				
			}
			System.out.println("genres done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//directedby
		query = "Select * from directedby;";
		int directorid = 0, movieid = 0;
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				movieid = rs.getInt("movieid");
				directorid = rs.getInt("directorid");
				
				document = new BasicDBObject("movieid", movieid)
                        .append("directorid", directorid);

				directedby.insert(document); 
				
			}
			System.out.println("directedby done");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//moviegenre
		query = "Select * from moviegenres;";
		int genreid = 0;
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				movieid = rs.getInt("movieid");
				genreid = rs.getInt("genreid");
				
				document = new BasicDBObject("genreid", genreid)
                        .append("movieid", movieid);

				moviegenres.insert(document); 
			}
			System.out.println("moviegenres done");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		query = "Select * from roles;";
		String role = "";
		int actorid = 0;
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				movieid = rs.getInt("movieid"); 
				actorid = rs.getInt("actorid");
				role = rs.getString("role");
				
				document = new BasicDBObject()
                        .append("movieid", movieid)
                        .append("actorid", actorid)
                        .append("role", role);

				roles.insert(document); 
								
			}
			
			System.out.println("roles done");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
 