import java.sql.*;  

public class plainIndex {

	public static void main(String[] args) {
		
		try{  
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "Your username";
			String password = "Your password";
			Connection connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump",username,password);    
			Statement statement = connect.createStatement();  
			
			System.out.println("The execution times without indexing are as follows:");
			System.out.println();
			executeQueries(statement);
			System.out.println();
			
			System.out.println("The execution time with indexing are as follows:");
			System.out.println();
			createIndexes(statement);
			executeQueries(statement);
			
			connect.close();  
		} 
		catch(Exception e){  
			System.out.println(e);
		}  
	}  
	
	public static void createIndexes(Statement statement) throws SQLException {
		statement.executeUpdate("create index directorIndex on directors(first, last);");
		statement.executeUpdate("create index genreIndex on genres(genre);");
		statement.executeUpdate("create index actorIndex on actors(first, last);");
	}
	
	public static void executeQueries(Statement statement) throws SQLException {
		String query1  = "select count(*) " + 
				"from roles, directors, directedby, moviegenres, genres, movies" + 
				"	where roles.movieid = movies.id " + 
				"		and movies.year between 1989 and 1995" + 
				"			and movies.id = moviegenres.movieid" + 
				"				and moviegenres.genreid = genres.id" + 
				"					and genres.genre = 'Sci-Fi'" + 
				"						and movies.id = directedby.movieid" + 
				"							and directedby.directorid = directors.id" + 
				"								and directors.first = 'James (I)'" + 
				"									and directors.last = 'Cameron';"; 
		System.out.println("Query1 : " + calculateExecutionTime(query1, statement) + " milliseconds");
		
		String query2  = "select movies.title " + 
				"from ( (movies join moviegenres on moviegenres.movieid = movies.id)" + 
				"join genres on genres.id = moviegenres.genreid" + 
				"	join directedby on directedby.movieid = movies.id" + 
				"		join directors on directors.id = directedby.directorid)" + 
				"			where genres.genre = 'Drama' and directors.first = 'Steven'" + 
				"				and directors.last = 'Spielberg';";
		
		System.out.println("Query2 : " + calculateExecutionTime(query2, statement) + " milliseconds");
		
		String query3  = "Select count(*)" + 
				"	from movies join roles on roles.movieid = movies.id " + 
				"		join actors on actors.id = roles.actorid" + 
				"		join directedby on directedby.movieid = movies.id" + 
				"		join directors on directors.id = directedby.directorid" + 
				"			where actors.first = 'Tom' and actors.last = 'Hanks'" + 
				"				and directors.first = 'Steven' and directors.last = 'Spielberg';"; 
		System.out.println("Query3 : " + calculateExecutionTime(query3, statement) + " milliseconds");
		
		String query4  = "select actors.first, actors.last" + 
				"	from actors join roles on roles.actorid = actors.id" + 
				"		join directedby on directedby.movieid = roles.movieid" + 
				"		join directors on directors.id = directedby.directorid" + 
				"		join moviegenres on moviegenres.movieid = roles.movieid" + 
				"		join genres on genres.id = moviegenres.genreid" + 
				"		join movies on movies.id = roles.movieid" + 
				"			where directors.first = 'Woody' and directors.last = 'Allen'" + 
				"				and genres.genre = 'Comedy'" + 
				"					group by roles.actorid" + 
				"						having count(roles.movieid) > 3" + 
				"							order by count(actors.first) desc;"; 
		System.out.println("Query4 : " + calculateExecutionTime(query4, statement) + " milliseconds");
		
		String query5  = "select m1.movieid from " + 
				"	(select movieid from roles, actors where roles.actorid = actors.id and actors.first = \"Leonardo\" and actors.last = \"DiCaprio\") as m1 " + 
				"		where m1.movieid in " + 
				"			(select movieid from roles, actors where roles.actorid = actors.id and actors.first = 'Kate' and actors.last = 'Winslet') ;";  
		System.out.println("Query5 : " + calculateExecutionTime(query5, statement) + " milliseconds");
	}
	
	public static long calculateExecutionTime(String query, Statement statement) throws SQLException {
		long t1 = System.currentTimeMillis();
		statement.executeQuery(query);
		long t2 = System.currentTimeMillis();
		
		return (t2 - t1);
	}
		
		
}
