import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class Denormalized {
	
	
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
//			populateMongoDB(mongoClient, statement);
			
			querying(mongoClient);
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
	
	public static void querying(MongoClient mongoClient) {

		DB database1 = mongoClient.getDB("IMDBDzzzz"); 
		DBCollection movies = database1.getCollection("movies");
		
		long t1, t2;
		
//		query1
		t1 = System.currentTimeMillis();
		BasicDBObject findquery1 = new BasicDBObject() 
				.append("year", new BasicDBObject().append("$gte", 1989).append("$lte", 1995)) 
				.append("directors", new BasicDBObject().append("first", "James (I)").append("last", "Cameron"))
				.append("genres", new BasicDBObject().append("genre", "Sci-Fi"));
		
		BasicDBObject unwind = new BasicDBObject().append("$unwind","$actors");
		BasicDBObject group = new BasicDBObject().append("$group",new BasicDBObject()
												.append("_id", null)
												.append("totalactors", new BasicDBObject().append("$push", "$actors")));
		BasicDBObject project = new BasicDBObject().append("$project", new BasicDBObject().append("_id", "0")
																			.append("number_of_actors", new BasicDBObject().append("$size", "$totalactors")));
		
		AggregationOutput results1 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery1), unwind, group, project)); 
		 
		System.out.println("Query 1 output:");
		System.out.println(results1.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
		
		
		
		
//		query 2
		t1 = System.currentTimeMillis();
		BasicDBObject findquery2 = new BasicDBObject()
				.append("directors", new BasicDBObject().append("first", "Steven").append("last", "Spielberg"))
				.append("genres", new BasicDBObject().append("genre", "Drama")); 
		
		group = new BasicDBObject().append("$group",new BasicDBObject()
				.append("_id", null)
				.append("totalmovies", new BasicDBObject().append("$push", "$title")));
		project = new BasicDBObject().append("$project", new BasicDBObject().append("_id", "0")
											.append("movies","$totalmovies"));

		AggregationOutput results2 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery2), group, project)); 
		System.out.println("Query 2 output:");
		System.out.println(results2.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
		
		
		
//		query 3
		t1 = System.currentTimeMillis();
		BasicDBObject findquery3 = new BasicDBObject()
				.append("actors", new BasicDBObject().append("$elemMatch", new BasicDBObject().append("first", "Tom").append("last", "Hanks")))
				.append("directors", new BasicDBObject().append("first", "Steven").append("last", "Spielberg"));
		
		
		group = new BasicDBObject().append("$group",new BasicDBObject()
				.append("_id", null)
				.append("totalmovies", new BasicDBObject().append("$push", "$title")));
		project = new BasicDBObject().append("$project", new BasicDBObject().append("_id", "0")
									.append("number_of_movies", new BasicDBObject().append("$size","$totalmovies")));
  
		AggregationOutput results3 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery3), group, project)); 
		
		System.out.println("Query 3 output:");
		System.out.println(results3.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		 
//		query 4
		t1 = System.currentTimeMillis();
		BasicDBObject findquery4 = new BasicDBObject()
				.append("directors", new BasicDBObject().append("first", "Woody").append("last", "Allen"))
				.append("genres", new BasicDBObject().append("genre", "Comedy"));
		unwind = new BasicDBObject().append("$unwind","$actors"); 
		project = new BasicDBObject().append("$project", new BasicDBObject().append("first", "$actors.first").append("last", "$actors.last"));
		group = new BasicDBObject().append("$group",new BasicDBObject()
												.append("_id", new BasicDBObject().append("first", "$first").append("last", "$last"))
												.append("count", new BasicDBObject().append("$sum", 1))
												);
		
		BasicDBObject findquery41 = new BasicDBObject().append("$match", new BasicDBObject().append("count", new BasicDBObject().append("$gt", 3)));
		
		AggregationOutput results4 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery4), unwind, project, group, findquery41)); 
		
		System.out.println("Query 4 output:");
		System.out.println(results4.results());
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
//		Query 5
		
		AggregationOutput results5 = movies.aggregate(Arrays.asList(
//				new BasicDBObject().append("$match", new BasicDBObject().append("actors", new BasicDBObject().append("actors.first", "Leonardo").append("actors.last","DiCaprio")))
				new BasicDBObject().append("$match", new BasicDBObject().append("actors", new BasicDBObject().append("$elemMatch", new BasicDBObject().append("first", "Leonardo").append("last", "DiCaprio"))))
//				new BasicDBObject().append("$lookup", new BasicDBObject().append("from","actors").append("localField","actorid").append("foreignField", "_id").append("as", "actorrolesdata"))
				,new BasicDBObject().append("$unwind","$actors")
				,new BasicDBObject().append("$match", new BasicDBObject().append("actors.first", "Kate").append("actors.last", "Winslet"))
				));
		int count = 0;
		for (DBObject result : results5.results()) {
			count = count + 1;
			System.out.println(result);
		}
		System.out.println("count: "+count);
		
	}
	
	
	public static void populateMongoDB(MongoClient mongoClient, Statement statement) {		
		
		// Creating DB
		DB database = mongoClient.getDB("IMDBDzzzz");
		 
		// Creating collections
		DBCollection movies = database.getCollection("movies");
		
		String query = ""; 
		int id = 0;
		DBObject document = null;
		
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
                        .append("year", year)
                        .append("actors", new ArrayList<BasicDBObject>())
                        .append("directors", new ArrayList<BasicDBObject>())
                        .append("genres", new ArrayList<BasicDBObject>());

				movies.insert(document); 
			}
			System.out.println("movies done");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		
		//directors
		query = "select distinct directors.first, directors.last, directedby.movieid " + 
				"from directors, directedby " + 
				"where directors.id = directedby.directorid;";
		
		String dfirst = "", dlast = "";
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("directedby.movieid");
				dfirst = rs.getString("directors.first");
				dlast = rs.getString("directors.last");
				
				BasicDBObject query1 = new BasicDBObject().append("_id",id);
				BasicDBObject director = new BasicDBObject().append("directors", new BasicDBObject().append("first", dfirst).append("last", dlast)); 
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append(("$push"), director);
				movies.update(query1, newDocument);							
			}
			
			System.out.println("directors done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		//actors		
		query = "select actors.first, actors.last, role, gender, roles.movieid " + 
				"from actors, roles " + 
				"where roles.actorid = actors.id";
		
		String afirst = "", alast = "", gender = "", role = "";
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("roles.movieid");
				afirst = rs.getString("actors.first");
				alast = rs.getString("actors.last");
				gender = rs.getString("gender");
				role = rs.getString("role");
				
				BasicDBObject query1 = new BasicDBObject().append("_id",id);
				BasicDBObject actor = new BasicDBObject()
										.append("actors", 
												new BasicDBObject()
													.append("first", afirst)
													.append("last", alast)
													.append("gender", gender)
													.append("role", role)); 
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append(("$push"), actor);
				movies.update(query1, newDocument);
												
			}
			
			System.out.println("actors done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		//genres
		query = "select moviegenres.movieid, genres.genre " + 
				"from genres, moviegenres " + 
				"where genres.id = moviegenres.genreid;";
		
		String genretype = "";
		try {
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("moviegenres.movieid");
				genretype = rs.getString("genres.genre");
				
				BasicDBObject query1 = new BasicDBObject().append("_id",id);
				BasicDBObject genres = new BasicDBObject()
										.append("genres", 
												new BasicDBObject()
													.append("genre", genretype)); 
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append(("$push"), genres);
				movies.update(query1, newDocument);
												
			}
			
			System.out.println("genres done");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	}
	
}
 