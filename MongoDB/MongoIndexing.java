import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

public class MongoIndexing {
	
	public static void main(String args[]) {
		
		Connection connect = null;
		try{  
			// MySQL
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "Your username";
			String password = "Your password";
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			Statement statement = connect.createStatement();  

			
			// MongoDB
			MongoClient mongoClient = new MongoClient();
			
			
			System.out.println("Normalized without Indexing: "); 
			Normalizedqueries(mongoClient);
			System.out.println();
			System.out.println("Normalized with indexing: ");
			createIndexesforNormalized(mongoClient);
			Normalizedqueries(mongoClient);
			System.out.println();

			
			System.out.println("DeNormalized without Indexing: ");
			DeNormalizedqueries(mongoClient);
			System.out.println();
			System.out.println("DeNormalized with indexing: ");
			createIndexesforDeNormalized(mongoClient);
			DeNormalizedqueries(mongoClient);
			System.out.println();
			
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
	
	public static void createIndexesforNormalized(MongoClient mongoClient) {
		DB database = mongoClient.getDB("IMDB"); 

		DBCollection directors = database.getCollection("directors");
		DBCollection genres = database.getCollection("genres");
		DBCollection actors = database.getCollection("actors");
	
		directors.createIndex(new BasicDBObject().append("first", 1).append("last", 1));
		genres.createIndex(new BasicDBObject().append("genre", 1));
		actors.createIndex(new BasicDBObject().append("first", 1).append("last", 1));
		
	}
	
	public static void createIndexesforDeNormalized(MongoClient mongoClient) {
		DB database1 = mongoClient.getDB("IMDBDzzzz"); 
		DBCollection movies = database1.getCollection("movies");
		
		movies.createIndex(new BasicDBObject().append("actors.first", 1).append("actors.last", 1));
		movies.createIndex(new BasicDBObject().append("directors.first", 1).append("directors.last", 1));
		movies.createIndex(new BasicDBObject().append("genres.genre", 1));
		
		System.out.println("done");
	}
	
	public static void DeNormalizedqueries(MongoClient mongoClient) {
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
//																			.append(key, val));
		 
		AggregationOutput results1 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery1), unwind, group, project)); 
		 
		System.out.println("Query 1 ");
		results1.results();
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
//											.append(key, val));

		AggregationOutput results2 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery2), group, project)); 
		System.out.println("Query 2 :");
		results2.results();
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
//											.append(key, val));
  
		AggregationOutput results3 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", findquery3), group, project)); 
		
		System.out.println("Query 3 :");
		results3.results();
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
		
		System.out.println("Query 4 :");
		results4.results();
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
		
//		Query 5
		t1 = System.currentTimeMillis();
		AggregationOutput results5 = movies.aggregate(Arrays.asList(
				new BasicDBObject().append("$match", new BasicDBObject().append("actors", new BasicDBObject().append("$elemMatch", new BasicDBObject().append("first", "Leonardo").append("last", "DiCaprio"))))
				,new BasicDBObject().append("$unwind","$actors")
				,new BasicDBObject().append("$match", new BasicDBObject().append("actors.first", "Kate").append("actors.last", "Winslet")) 
				));
		int count = 0;
		System.out.println("Query 5 :");
		for (DBObject result : results5.results()) {
			count = count + 1;
			System.out.println(result);
		}
		System.out.println("count: "+count);
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		
	}
	
	public static void Normalizedqueries(MongoClient mongoClient) {
		
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
		
		results1.results();
		System.out.println("Query 1 : ");
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
		results2.results();
		System.out.println("Query 2 : ");
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println();
		int count = 0;
		for (DBObject result : results2.results()) {
			count = count + 1;
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
		results3.results();
		System.out.println("Query 3 : ");
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
	 	
		count = 0; 
		for (DBObject result : results4.results()) {
			count = count + 1;
		}
		System.out.println("Number of actors: "+count);
		System.out.println("Query 4 : ");
		t2 = System.currentTimeMillis();
		System.out.println("Time taken: "+ (t2-t1)+" milliseconds");
		System.out.println(); 
		 
	}
	

		
	
	
}
 