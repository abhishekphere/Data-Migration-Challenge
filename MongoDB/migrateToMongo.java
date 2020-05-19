import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class migrateToMongo {
	
	
	public static void main(String args[]) {
		
		Connection connect = null;
		try{  
			// MySQL
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "your username";
			String password = "your password";
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			Statement statement = connect.createStatement();  

			
			
			// MongoDB
			MongoClient mongoClient = new MongoClient();
			
			// populating Mongo database 
			populateMongoDB(mongoClient, statement);
			
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
		
	}
	
}
 