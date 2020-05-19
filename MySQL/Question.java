import java.sql.*;  

public class Question {

	public static void main(String[] args) {
		
		try{  
			Class.forName("com.mysql.jdbc.Driver");  
			String username = "root";
			String password = "01973";
			Connection connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump",username,password);    
			
			System.out.println("The execution times without indexing are as follows:");
			System.out.println();
			executeQueries(connect);
			System.out.println();
			
			System.out.println();
			
			connect.close();  
		} 
		catch(Exception e){  
			System.out.println(e);
		}  
	}  
	
	public static void executeQueries(Connection connect) throws SQLException {
		
		Statement statement = connect.createStatement();
		String query1  = "select count(*) as c " + 
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
		
		ResultSet rs = statement.executeQuery(query1);
		int count = 0;
		System.out.println("Output of query 1:");
		while(rs.next()) {
			count = rs.getInt("c");
		}
		System.out.println(count);
		System.out.println();
		statement.close();
		rs.close();
		
		String query2  = "select movies.title " + 
				"from ( (movies join moviegenres on moviegenres.movieid = movies.id)" + 
				"join genres on genres.id = moviegenres.genreid" + 
				"	join directedby on directedby.movieid = movies.id" + 
				"		join directors on directors.id = directedby.directorid)" + 
				"			where genres.genre = 'Drama' and directors.first = 'Steven'" + 
				"				and directors.last = 'Spielberg';";
		statement = connect.createStatement(); 
		rs = statement.executeQuery(query2);
		
		String movie = null;
		System.out.println("Output of query 2:");
		count = 0;
		while(rs.next()) {
			count++;
			movie = rs.getString("movies.title");
			System.out.println(movie);
		}
		System.out.println("Number of rows: "+count);
		System.out.println();
		statement.close();
		rs.close();
		
		String query3  = "Select count(*) as c " + 
				"	from movies join roles on roles.movieid = movies.id " + 
				"		join actors on actors.id = roles.actorid" + 
				"		join directedby on directedby.movieid = movies.id" + 
				"		join directors on directors.id = directedby.directorid" + 
				"			where actors.first = 'Tom' and actors.last = 'Hanks'" + 
				"				and directors.first = 'Steven' and directors.last = 'Spielberg';"; 
		
		statement = connect.createStatement(); 
		rs = statement.executeQuery(query3);
		
		count = 0;
		System.out.println("Output of query 3:");
		while(rs.next()) {
			count = rs.getInt("c");
		}
		System.out.println(count);
		System.out.println();
		statement.close();
		rs.close();
		
		
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
		
		statement = connect.createStatement(); 
		rs = statement.executeQuery(query4);
		String first = null, last = null;
		count = 0;
		System.out.println("Output of query 4:");
		while(rs.next()) {
			count++;
			first = rs.getString("actors.first");
			last = rs.getString("actors.last");
			System.out.println(first+"  "+last);
		}
		System.out.println("Number of rows: "+count);

		System.out.println();
		statement.close();
		rs.close();
		
		
		String query5  = "select m1.movieid from " + 
				"	(select movieid from roles, actors where roles.actorid = actors.id and actors.first = \"Leonardo\" and actors.last = \"DiCaprio\") as m1 " + 
				"		where m1.movieid in " + 
				"			(select movieid from roles, actors where roles.actorid = actors.id and actors.first = 'Kate' and actors.last = 'Winslet') ;"; 
		
		statement = connect.createStatement(); 
		rs = statement.executeQuery(query5);
		count = 0;
//		movie = null;
		int id =0;
		System.out.println("Output of query 5:");
		while(rs.next()) {
			count++;
			id = rs.getInt("m1.movieid");
			System.out.println(id);
		}
		System.out.println("Number of rows: "+count);

		System.out.println();
		statement.close();
		rs.close();
		
	}
	
	
}
