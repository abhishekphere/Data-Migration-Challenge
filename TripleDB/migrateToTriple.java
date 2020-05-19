import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.tdb.TDBFactory; 
import org.apache.jena.vocabulary.RDF;

public class migrateToTriple {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		
		String directory = "Your directory name";
		Dataset dataset = TDBFactory.createDataset(directory);
		
		Model model = dataset.getDefaultModel();
		
		
		Connection connect = null;
		try{  
			// MySQL
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "root";
			String password = "01973";
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			java.sql.Statement statement = connect.createStatement();  

			addMovies(statement, model);
			addDirectors(statement, model); 
			addGenres(statement, model);
			addActors(statement, model);
			
			System.out.println("Done"); 
			 
			connect.close();
		}
		catch(Exception e){  
			System.out.println(e);
		}
		
	}
	
	
	public static void addMovies(java.sql.Statement statement, Model model) {
		String query = "Select * from movies;";
		String title = "";
		int id = 0, year = 0;  
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		Resource mainMovieSubject = model.createResource("https://Movies.data/Movies");
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				id = rs.getInt("id");
				title = rs.getString("title");
				year = rs.getInt("year");
				
				subject = model.createResource("https://fake.data/Movie-"+id);
				predicate = RDF.type;  
				model.add(subject, predicate, mainMovieSubject);
				
				predicate = model.createProperty("https://fake.model/hasTitle");
				object = model.createLiteral(title);
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasId");
				object = model.createLiteral(Integer.toString(id));
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasYear");
				object = model.createLiteral(Integer.toString(year));
				model.add(subject, predicate, object);
				
				subject = null;
				predicate = null;
				object = null;
//				System.gc();
				
			}
			System.out.println("movies done");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void addActors(java.sql.Statement statement, Model model) {
		
		String query = "Select * from actors;";
		
		
		Resource mainActorSubject = model.createResource("https://Actors.data/Actors");
		
		String first = "", last = "", gender = "";
		int actorid = 0, movieid = 0;  
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				
				actorid = rs.getInt("id"); 
				gender = rs.getString("gender");
				first = rs.getString("first");
				last = rs.getString("last");
				
				subject = model.createResource( "https://fake.data/Actor-" + actorid );
				predicate = RDF.type; 
				model.add(subject, predicate, mainActorSubject);
				
				predicate = model.createProperty("https://fake.model/hasId");
				object = model.createLiteral(Integer.toString(actorid)); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasFirst");
				object = model.createLiteral(first); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasLast");
				object = model.createLiteral(last); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasGender");
				object = model.createLiteral(gender); 
				model.add(subject, predicate, object);
				
				subject = null;
				predicate = null; 
				object = null;
				gender = null;
				first = null;
				last = null;
				
			}
			System.out.println("Actors done");
			rs = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	
		
		query = "select actors.id, roles.movieid from actors join roles on roles.actorid = actors.id";
	  
		subject = null;
		predicate = null;
		object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				
				actorid = rs.getInt("actors.id");
				movieid = rs.getInt("roles.movieid");
				
				subject = model.createResource( "https://fake.data/Actor-"+ actorid );
				Resource movieSubject = model.createResource("https://fake.data/Movie-"+ movieid);
				predicate = model.createProperty("https://fake.data/hasActor"); 
				model.add(movieSubject, predicate, subject); 
				
				subject = null;
				predicate = null;
				object = null;
				
			}
			System.out.println("Actors directors done");
			rs = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void addDirectors(java.sql.Statement statement, Model model) {
		
		String query = "select * from directors";
		
		Resource mainDirectorSubject = model.createResource("https://Directors.data/Directors");
		
		String first = "", last = "";
		int directorid = 0, movieid = 0;  
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				directorid = rs.getInt("id");
				first = rs.getString("first");
				last = rs.getString("last");

				
				subject = model.createResource( "https://fake.data/Director-"+ directorid );
				predicate = RDF.type; 
				model.add(subject, predicate, mainDirectorSubject);
				
				predicate = model.createProperty("https://fake.model/hasId");
				object = model.createLiteral(Integer.toString(directorid)); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasFirst");
				object = model.createLiteral(first); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasLast");
				object = model.createLiteral(last); 
				model.add(subject, predicate, object); 
				
				subject = null;
				predicate = null; 
				object = null;
				first = null;
				last = null;
				 
			}
			System.out.println("Directors done");
			rs = null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		query = "select directors.id, directedby.movieid from directedby join directors on directors.id = directedby.directorid;";
		
		subject = null;
		predicate = null;
		object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				
				directorid = rs.getInt("directors.id");
				movieid = rs.getInt("directedby.movieid");
				
				subject = model.createResource( "https://fake.data/Director-"+ directorid );
				Resource movieSubject = model.createResource("https://fake.data/Movie-"+ movieid);
				predicate = model.createProperty("https://fake.data/hasDirector"); 
				model.add(movieSubject, predicate, subject); 
				
				subject = null;
				predicate = null;
				object = null;
				
			}
			System.out.println(" directors movies done");
			rs = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static void addGenres(java.sql.Statement statement, Model model) {
		String query = "select * from genres;";
		Resource mainGenreSubject = model.createResource("https://Genres.data/Genres"); 
		
		String genre = "";
		int genreid = 0, movieid = 0;  
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				genre = rs.getString("genre");
				genreid = rs.getInt("genres.id");
				
				
				subject = model.createResource( "https://fake.data/Genre-"+ genreid );
				predicate = RDF.type;
				model.add(subject, predicate, mainGenreSubject);
				
				predicate = model.createProperty("https://fake.model/hasId");
				object = model.createLiteral(Integer.toString(genreid)); 
				model.add(subject, predicate, object);
				
				predicate = model.createProperty("https://fake.model/hasGenreType");
				object = model.createLiteral(genre);   
				model.add(subject, predicate, object);
				
				subject = null;
				predicate = null;
				object = null; 
			}
			System.out.println("Genres done");
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
		
		
		 
		query = "select genres.id, moviegenres.movieid from genres join moviegenres on genres.id = moviegenres.genreid";
		
		genreid = 0;
		movieid = 0;  
		subject = null;
		predicate = null;
		object = null;
		
		try {
			java.sql.ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				movieid = rs.getInt("moviegenres.movieid");
				genreid = rs.getInt("genres.id");
				
				Resource movieSubject = model.createResource("https://fake.data/Movie-"+ movieid);
				subject = model.createResource( "https://fake.data/Genre-"+ genreid );
				predicate = model.createProperty("https://fake.data/hasGenre");
				model.add(movieSubject, predicate, subject);
				
				subject = null;
				predicate = null;
				object = null; 
			}
			System.out.println("Movies Genres done");
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
		
	}
	 
	

}


