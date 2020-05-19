import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class CreateIndexes {

	public static void main(String[] args) throws IOException {
//		dropIndexes();
		
		GraphDatabaseFactory dbFac = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFac.newEmbeddedDatabase(new File("Data file name"));
		System.out.println("Connected to db");
		System.out.println();
		
		query1(db);
		query2(db);
		query3(db);
		query4(db);
		query5(db);
		db.shutdown();
		
		createIndexes();
		
		db = dbFac.newEmbeddedDatabase(new File("Data file name"));
		query1(db);
		query2(db);
		query3(db);
		query4(db);
		query5(db);
		
		db.shutdown();
		System.out.println("Db shutdown");
		System.out.println("Done"); 
	}
	
	public static void createIndexes() {
		GraphDatabaseFactory dbFac = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFac.newEmbeddedDatabase(new File("Data file name"));
		
		db.execute("CREATE INDEX ON :Movie(title, year)");
		db.execute("CREATE INDEX ON :Actor(first, last)");
		db.execute("CREATE INDEX ON :Director(first, last)");
		db.execute("CREATE INDEX ON :Genre(genre)");
		System.out.println("Indexes created!");
	
		db.shutdown(); 
	}
	
	public static void dropIndexes() {
		GraphDatabaseFactory dbFac = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFac.newEmbeddedDatabase(new File("Data file name"));
		
		db.execute("DROP INDEX ON :Movie(title, year)");
		db.execute("DROP INDEX ON :Actor(first, last)");
		db.execute("DROP INDEX ON :Director(first, last)");
		db.execute("DROP INDEX ON :Genre(genre)");
		
		System.out.println("Dropped Indexes");
		db.shutdown(); 
	}
	
	public static void query1(GraphDatabaseService db) {
		System.out.println("Query 1:");
		long t1 = System.currentTimeMillis();
		
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (a:Actor), (m:Movie) --> (g:Genre) "
									+ " where d.first = \"James (I)\" and d.last = \"Cameron\""
									+ " and g.genre = \"Sci-Fi\""
									+ " and m.year > 1988 and m.year < 1996 return count(a.id) as count");
		
		Map<String, Object> data = res.next();
		System.out.println("Number of Actors: " + data.get("count"));
		
		long t2 = System.currentTimeMillis();
		System.out.println("Time Taken: " + (t2-t1) +" millis");
		System.out.println();
		
		res.close();
//		db.shutdown(); 
	}
	
	public static void query2(GraphDatabaseService db) {
		System.out.println("Query 2:");
		System.out.println("Movies are as follows:");
		long t1 = System.currentTimeMillis();
		
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (g:Genre) "
									+ " where d.first = \"Steven\" and d.last = \"Spielberg\""
									+ " and g.genre = \"Drama\" return m.title");
		
		int c = 0;
		while(res.hasNext()) {
			Map<String, Object> data = new HashMap<>();
			data = res.next();
			c++;
			System.out.println(data.get("m.title"));
		}
		System.out.println("count: " + c);
		
		long t2 = System.currentTimeMillis();
		System.out.println("Time Taken: " + (t2-t1) +" millis");
		System.out.println();
		
		res.close(); 
	}
	
	public static void query3(GraphDatabaseService db) {
		System.out.println("Query 3:");
		long t1 = System.currentTimeMillis();
		
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (a:Actor) "
									+ " where d.first = \"Steven\" and d.last = \"Spielberg\""
									+ " and a.first = \"Tom\" and a.last = \"Hanks\" return count(m.id) as count");
		Map<String, Object> data = res.next();
		System.out.println("Number of Movies: "+data.get("count"));
		
		long t2 = System.currentTimeMillis();
		System.out.println("Time Taken: " + (t2-t1) +" millis");
		System.out.println();
		res.close();
	}
	
	public static void query4(GraphDatabaseService db) {
		System.out.println("Query 4:");
		System.out.println("Names of actors are as follows: ");
		long t1 = System.currentTimeMillis();
		
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (a:Actor), (m:Movie) --> (g:Genre) "
									+ " where d.first = \"Woody\" and d.last = \"Allen\""
									+ " and g.genre = \"Comedy\"  with count(m.id) as count, a.first as afirst, a.last as alast "
									+ " where count > 3 "
									+ " return count, afirst, alast order by count desc ");
		
		int c = 0;
		while(res.hasNext()) {
			Map<String, Object> data = new HashMap<>();
			data = res.next();
			c++;
			System.out.println(data.get("count") +" "+ data.get("afirst")+" "+ data.get("alast"));
		}
		System.out.println("count: " + c);

		long t2 = System.currentTimeMillis();
		System.out.println("Time Taken: " + (t2-t1) +" millis");
		System.out.println();
		res.close();
	}
	
	public static void query5(GraphDatabaseService db) {
		System.out.println("Query 5:");
		System.out.println("Movies are as follows: ");
		long t1 = System.currentTimeMillis();

		Result res = db.execute(" Match (a1:Actor) -- (m1:Movie) -- (a:Actor) -- (m2:Movie) -- (a2:Actor) "
				+ " where a1.first = \"Kate\" and a1.last = \"Winslet\" "
				+ " and a2.first = \"Leonardo\" and a2.last = \"DiCaprio\" "
				+ " and not a1 = a and not a2 = a "
				+ " unwind [m1.title, m2.title ] as movies "
				+ " return distinct movies"
				+ " union "
				+ " Match (a1:Actor) -- (m1:Movie) -- (a3:Actor) -- (m3:Movie) -- (a4:Actor) -- (m2:Movie) -- (a2:Actor) "
				+ " where a1.first = \"Kate\" and a1.last = \"Winslet\" "
				+ " and a2.first = \"Leonardo\" and a2.last = \"DiCaprio\" "
				+ " and not a1 = a3 and not a2 = a3 and not a1 = a4 and not a2 = a4 "
				+ " unwind [m1.title, m2.title, m3.title] as movies "
				+ " return distinct movies"
				+ " union "
				+ " Match (a1:Actor) -- (m1:Movie) -- (a3:Actor) -- (m4:Movie) -- (a5:Actor) -- (m3:Movie) -- (a4:Actor) -- (m2:Movie) -- (a2:Actor) "
				+ " where a1.first = \"Kate\" and a1.last = \"Winslet\" "
				+ " and a2.first = \"Leonardo\" and a2.last = \"DiCaprio\" "
				+ " and not a1 = a3 and not a2 = a3 and not a1 = a4 and not a2 = a4 "
				+ " unwind [m1.title, m2.title, m3.title, m4.title] as movies "
				+ " return distinct movies");
		
		int c = 0;
		while(res.hasNext()) {
			Map<String, Object> data = new HashMap<>();
			data = res.next();
			c++; 
			System.out.println(data.get("movies") );
		}
		System.out.println("count: " + c);
		
		long t2 = System.currentTimeMillis();
		System.out.println("Time Taken: " + (t2-t1) +" millis");
		System.out.println();
		res.close(); 
	}
	
}