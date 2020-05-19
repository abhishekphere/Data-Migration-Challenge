import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.sql.Connection; 
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class migrateToNeo {

	public static void main(String[] args) throws IOException {
		
		Connection sqlConnect = null;
		try{  
			// MySQL
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "Your username";
			String password = "Your password";
			sqlConnect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			Statement statement = sqlConnect.createStatement();  			
			sqlConnect.close();
		}  
		catch(Exception e){  
			System.out.println(e);
		}		
	}
	
	public static void addMoviesNodes(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from movies;";
		Map<String, Object> movieData = null;
		
		String title = "";
		String movieid = "";
		int year = 0; 
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				title = rs.getString("title");
				movieid = Integer.toString(rs.getInt("id")) + "00"; 
				year = rs.getInt("year");
				
				movieData = new HashMap<>();
				movieData.put("id", rs.getInt("id"));
				movieData.put("title", title);
				movieData.put("year", year);
				
				inserter.createNode(Long.valueOf(movieid), movieData, Label.label("Movie")); 
				
			}
			System.out.println("Movies done");
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}
		
	}
	
	
	public static void addGenresNodes(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from genres;";
		Map<String, Object> genreData = null;
		
		String genre = "";
		int genreid = 0;  
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				genre = rs.getString("genre");
				genreid = rs.getInt("id");
				genreData = new HashMap<>();
				genreData.put("id", genreid);
				genreData.put("genre", genre);
								
				inserter.createNode(Long.valueOf(genreid), genreData, Label.label("Genre")); 				
			}
			System.out.println("Genres done");
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}		
	}
	
	
	public static void addActorsNodes(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from actors;";
		Map<String, Object> actorData = null;
		
		String actorid = "";
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				actorid = Integer.toString(rs.getInt("id"))+"5";
				
				actorData = new HashMap<>();
				actorData.put("id", rs.getInt("id"));
				actorData.put("first", rs.getString("first"));
				actorData.put("last", rs.getString("last"));
				actorData.put("gender", rs.getString("gender"));
				
				inserter.createNode(Long.valueOf(actorid), actorData, Label.label("Actor")); 
				
			}
			System.out.println("Actors done");
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}
		
	}
	
	public static void addDirectorsNodes(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from directors;";
		Map<String, Object> directorData = null;
		
		String directorid = "";
		int length = 0;
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				directorid = Integer.toString(rs.getInt("id")) + "6";
				directorData = new HashMap<>();
				directorData.put("id", rs.getInt("id"));
				directorData.put("first", rs.getString("first"));
				directorData.put("last", rs.getString("last"));
				
				inserter.createNode(Long.valueOf(directorid), directorData, Label.label("Director")); 
				directorid = null;
			}
			System.out.println("Directors done");
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}
		
	}
	
	public static void addMovieGenresRelation(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from moviegenres;";
		Map<String, Object> genreData = null;
		
		int genreid = 0;  
		String movieid ="";
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				genreid = rs.getInt("genreid");
				movieid = Integer.toString(rs.getInt("movieid")) + "00";
				
				genreData = new HashMap<>();
				genreData.put("genreid", genreid);
								
				inserter.createRelationship(Long.valueOf(movieid), Long.valueOf(genreid), RelationshipType.withName("hasGenreType"), genreData); 
			}
			System.out.println("MovieGenres done");
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}
		
	}
	
	
	public static void addMovieDirectorsRelation(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from directedby;";
		Map<String, Object> attribute = null;
		
		String movieid ="", directorid = "";
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				directorid = Integer.toString(rs.getInt("directorid")) + "6";
				movieid = Integer.toString(rs.getInt("movieid")) + "00";
				
				attribute = new HashMap<>();
				attribute.put("directeorid", rs.getInt("directorid"));  
								
				inserter.createRelationship(Long.valueOf(movieid), Long.valueOf(directorid), RelationshipType.withName("hasDirector"), attribute); 
			}
			System.out.println("MovieDirector done"); 
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally {
			inserter.shutdown();
		}
		
	}
	
	public static void addMovieActorsRelation(Statement statement) throws IOException {
		
		BatchInserter inserter = BatchInserters.inserter(new File("MyData1"));
		
		String query = "select * from roles;";
		Map<String, Object> attribute = null;
		
		String movieid ="", actorid = "";
		
		try {  
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				actorid = Integer.toString(rs.getInt("actorid")) + "5";
				movieid = Integer.toString(rs.getInt("movieid")) + "00";
				
				attribute = new HashMap<>();
				attribute.put("actorid", rs.getInt("actorid"));  
								
				inserter.createRelationship(Long.valueOf(movieid), Long.valueOf(actorid), RelationshipType.withName("hasActor"), attribute); 
			}
			System.out.println("MovieActor done");  
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		finally { 
			inserter.shutdown();
		}
	}
	
}
