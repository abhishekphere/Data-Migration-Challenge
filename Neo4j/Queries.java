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

public class Queries {

	public static void main(String[] args) throws IOException {
		
		GraphDatabaseFactory dbFac = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFac.newEmbeddedDatabase(new File("data file name"));
		System.out.println("connected");
		query1(db);
		query2(db);
 		query3(db);
 		query4(db);
 		query5(db); 
				
		GraphDatabaseFactory dbFac = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFac.newEmbeddedDatabase(new File("data file name"));
		Result res = db.execute("");
		res.close();
		db.shutdown();
		 
		
	}
	
	public static void query1(GraphDatabaseService db) {
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (a:Actor), (m:Movie) --> (g:Genre) "
									+ " where d.first = \"James (I)\" and d.last = \"Cameron\""
									+ " and g.genre = \"Sci-Fi\""
									+ " and m.year > 1988 and m.year < 1996 return count(a.id) as count");
		
		Map<String, Object> data = res.next();
		System.out.println(data.get("count"));
		
		res.close();
		db.shutdown(); 
	}
	
	public static void query2(GraphDatabaseService db) {
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (g:Genre) "
									+ " where d.first = \"Steven\" and d.last = \"Spielberg\""
									+ " and g.genre = \"Drama\" return m.title");
		
//		Map<String, Object> data = res.next();
		int c = 0;
		while(res.hasNext()) {
			Map<String, Object> data = new HashMap<>();
			data = res.next();
			c++;
			System.out.println(data.get("m.title"));
		}
		System.out.println("count: " + c);
		
		res.close();
		db.shutdown(); 
	}
	
	public static void query3(GraphDatabaseService db) {
		Result res = db.execute("Match (m:Movie) --> (d:Director), (m:Movie) --> (a:Actor) "
									+ " where d.first = \"Steven\" and d.last = \"Spielberg\""
									+ " and a.first = \"Tom\" and a.last = \"Hanks\" return count(m.id) as count");
		Map<String, Object> data = res.next();
		System.out.println(data.get("count"));
		
		res.close();
		db.shutdown();
	}
	
	public static void query4(GraphDatabaseService db) {
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
		
		res.close();
		db.shutdown(); 
	}
	
	public static void query5(GraphDatabaseService db) {
		
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
		
		res.close();
		db.shutdown(); 
	}
	
	
}