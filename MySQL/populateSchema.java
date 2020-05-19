import java.sql.*;  

public class populateSchema {

	public static void main(String[] args) {
		
		
		Connection connect = null;
		try{  
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "root";
			String password = "01973";
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump", username, password);    
			Statement statement = connect.createStatement();  
			createDatabase(statement);
			createTables(statement);
//			populateSchema(statement); 
			System.out.println("Done"); 
			
			connect.close();   
		} 
		catch(Exception e){  
			System.out.println(e);
		} 
		
	} 
	
	public static void createDatabase(Statement statement) throws SQLException {
		String query = "CREATE DATABASE newSchema;";
		
		statement.execute(query);
		statement.execute("Use newSchema;");
	} 
	
	public static void createTables(Statement statement) throws SQLException {
		 
		 String table1 = "create table Person (person_id varchar(255) not NULL, "+
				 			"name varchar(255), PRIMARY KEY (person_id));";
		 statement.executeUpdate(table1);
		 
		 String table2 = "create table Movie (movie_id int not NULL, "+
		 			"title varchar(255), release_year int, PRIMARY KEY (movie_id));";
		 statement.executeUpdate(table2);
		 
		 String table3 = "create table Director ( person_id varchar(255), movie_id int, "
		 		+ "PRIMARY KEY(person_id, movie_id),"+
		 			"FOREIGN KEY (person_id) REFERENCES Person(person_id),"+
		 			"FOREIGN KEY (movie_id) REFERENCES Movie(movie_id));";
		 statement.executeUpdate(table3);
		 
		 String table4 = "create table Actor ( person_id varchar(255), movie_id int, "
		 		+ "PRIMARY KEY(person_id, movie_id),"+
		 			"FOREIGN KEY (person_id) REFERENCES Person(person_id),"+
		 			"FOREIGN KEY (movie_id) REFERENCES Movie(movie_id));";
		 statement.executeUpdate(table4);
	}
	
	public static void populateSchema(Statement statement) throws SQLException {
		
		String query = "Set FOREIGN_KEY_CHECKS=0;";
		statement.execute(query);
		
		String query1 = "Insert into newschema.person(person_id, name)"+ 
						"select concat(id, '_director'), concat(first,' ', last) from imdb_dump.directors;";
		statement.executeUpdate(query1);

		String query2 = "Insert into newschema.person(person_id, name)"+ 
						"select concat(id, '_actor'), concat(first,' ', last) from imdb_dump.actors;";
		statement.executeUpdate(query2);
		
		String query3 = "Insert into newschema.movie(movie_id, title, release_year)"+ 
						"select id, title, year from imdb_dump.movies;";
		statement.executeUpdate(query3);

		String query4 = "Insert into newschema.director(movie_id, person_id)"+ 
						"select movieid, concat(directorid,\"_director\") from imdb_dump.directedby;";
		statement.executeUpdate(query4);
		
		String query5 = "Insert into newschema.actor(movie_id, person_id)"+  
						"select movieid, concat(actorid,\"_actor\") from imdb_dump.roles;";
		statement.executeUpdate(query5);
		
	}

}
