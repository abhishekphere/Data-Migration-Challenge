import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.tdb.TDBFactory; 
import org.apache.jena.vocabulary.RDF;

public class TripleQueries {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		
		String directory = "Your directory name"; 
		Dataset dataset = TDBFactory.createDataset(directory);
		
		Model model = dataset.getDefaultModel();
		
		query1(model);
		query2(model);
		query3(model);
		query4(model);
		query5(model);
		
	}
	
	public static void query1(Model model) {
		
		Query query = QueryFactory.create("Select (count(?a) as ?total) "
				+ "where { "
				+ 	" ?m <https://fake.data/hasActor> ?a ."
				+ 	" ?m <https://fake.data/hasDirector> ?d ."
				+ 	" ?d <https://fake.model/hasFirst> \"James (I)\" ."
				+ 	" ?d <https://fake.model/hasLast> \"Cameron\" ." 
				+ 	" ?m <https://fake.data/hasGenre> ?g ."
				+ 	" ?g <https://fake.model/hasGenreType> \"Sci-Fi\" ."
				+ 	" ?m <https://fake.model/hasYear> ?year ."  
				+ 	" filter (?year > \"1988\" && ?year < \"1996\") ." 
				+ "}");
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		while(rs.hasNext()) {
			i++;
			QuerySolution sol = rs.next();
			System.out.println(sol.get("?total"));
		} 
		qexec.close();
		System.out.println("count: "+i);
		
	}
	
	public static void query2(Model model) {
		
		Query query = QueryFactory.create("Select ?m "
				+ "where { "
				+ 	" ?m <https://fake.data/hasDirector> ?d ."
				+ 	" ?d <https://fake.model/hasFirst> \"Steven\" ."
				+ 	" ?d <https://fake.model/hasLast> \"Spielberg\" ."
				+ 	" ?m <https://fake.data/hasGenre> ?g ."
				+ 	" ?g <https://fake.model/hasGenreType> \"Drama\" ."
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		while(rs.hasNext()) {
			i++;
			QuerySolution sol = rs.next();
			System.out.println("Subject found: " + sol.get("?m"));
		}
		qexec.close();
		System.out.println("count: "+i);
		
	}
	
	public static void query3(Model model) {
		
		Query query = QueryFactory.create("Select count(?m) as ?total"
				+ "where { "
				+ 	" ?m <https://fake.data/hasActor> ?a ."
				+ 	" ?m <https://fake.data/hasDirector> ?d ."
				+ 	" ?d <https://fake.model/hasFirst> \"Steven\" ."
				+ 	" ?d <https://fake.model/hasLast> \"Spielberg\" ."
				+	" ?a <https://fake.model/hasFirst> \"Tom\" ."
				+	" ?a <https://fake.model/hasLast> \"Hanks\" ."
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		while(rs.hasNext()) {
			i++;
			QuerySolution sol = rs.next();
			System.out.println("Subject found: " + sol.get("?total"));
		}
		qexec.close();
		System.out.println("count: "+i);
		
	}

	public static void query4(Model model) {
		
		Query query = QueryFactory.create("Select ?a "
				+ "where { "
				+ 	" ?m <https://fake.data/hasActor> ?a ."
				+ 	" ?m <https://fake.data/hasDirector> ?d ."
				+ 	" ?d <https://fake.model/hasFirst> \"Woody\" ."
				+ 	" ?d <https://fake.model/hasLast> \"Allen\" ."
				+ 	" ?m <https://fake.data/hasGenre> ?g ."
				+ 	" ?g <https://fake.model/hasGenreType> \"Comedy\" ." 
				+ "}"
				+ "group by ?a "
				+ "having (count(?m) > 3)");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		while(rs.hasNext()) {
			i++;
			QuerySolution sol = rs.next();
			System.out.println("Subject found: " + sol.get("?a"));
		}
		qexec.close();
		System.out.println("count: "+i);
		
	}
	
	public static void query5(Model model) {
		
		Query query = QueryFactory.create("Select ?m1 "
				+ "where { "
				+ 	" ?kw <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Actors.data/Actors> ."
				+ 	" ?kw <https://fake.model/hasFirst> \"Kate\" ."
				+ 	" ?kw <https://fake.model/hasLast> \"Winslet\" ."
				+ 	" ?lc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Actors.data/Actors> ."
				+ 	" ?lc <https://fake.model/hasFirst> \"Leonardo\" ."
				+ 	" ?lc <https://fake.model/hasLast> \"DiCaprio\" ."
				+   " ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Actors.data/Actors> ."
				+ 	" ?m1 <https://fake.data/hasActor> ?a ."
				+ 	" ?m2 <https://fake.data/hasActor> ?a ."
				+ 	" ?m1 <https://fake.data/hasActor> ?kw ."
				+ 	" ?m2 <https://fake.data/hasActor> ?lc ."
				+ 	" filter (?a != ?kw && ?a != ?lc) ." 
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		while(rs.hasNext()) {
			i++;
			QuerySolution sol = rs.next();
			System.out.println("Subject found: " + sol.get("?m1"));
		}
		qexec.close();
		System.out.println("count: "+i);
		
	}
	

}


