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
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDF;

public class populateNewSchema {

	public static void main(String[] args) { 
		
		String directory = "Your directory name";  
		Dataset dataset = TDBFactory.createDataset(directory);
		
		String directoryNew = "New directory name";
		Dataset datasetNew = TDBFactory.createDataset(directoryNew);  
		
		Model model1 = dataset.getDefaultModel();
		Model model2 = datasetNew.getDefaultModel();
		
		createNewSchema(model1, model2);
		
	}
	
	
	public static void createNewSchema(Model model1, Model model2) {
		
//		create movies
		createMovies(model1, model2);
		createPerson(model1, model2);
		createActors(model1, model2);
		createDirectors(model1, model2);
	}
	
	public static void createMovies(Model model1, Model model2) {
		
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		Resource mainMovieSubject = model2.createResource("https://Movies.data/Movies");
		
		Query query = QueryFactory.create("Select ?id ?title ?year "
				+ "where { "
				+ 	" ?m <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Movies.data/Movies> ."
				+ 	" ?m <https://fake.model/hasId> ?id ."
				+ 	" ?m <https://fake.model/hasTitle> ?title ."
				+ 	" ?m <https://fake.model/hasYear> ?year ." 
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model1);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		String id="", title="", year="";
		while(rs.hasNext()) {
			QuerySolution sol = rs.next();
			id = ""+sol.get("?id");
			title = ""+sol.get("?title");
			year = ""+sol.get("?year");
			
			subject = model2.createResource("https://fake.data/Movie-"+id);
			predicate = RDF.type;  
			model2.add(subject, predicate, mainMovieSubject);
			
			predicate = model2.createProperty("https://fake.model/hasTitle");
			object = model2.createLiteral(title);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasId");
			object = model2.createLiteral(id);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasYear");
			object = model2.createLiteral(year);
			model2.add(subject, predicate, object);
			
			subject = null;
			predicate = null;
			object = null;
			
			i++;
		}
		qexec.close();
		qexec = null;
		System.out.println("Movies done");
	}
	
	public static void createPerson(Model model1, Model model2) {
		
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		Resource mainPersonSubject = model2.createResource("https://Person.data/Person");
		// Filling actors first
		Query query = QueryFactory.create("Select ?id ?first ?last "
				+ "where { "
				+ 	" ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Actors.data/Actors> ."
				+ 	" ?a <https://fake.model/hasId> ?id ."
				+ 	" ?a <https://fake.model/hasFirst> ?first ."
				+ 	" ?a <https://fake.model/hasLast> ?last ." 
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model1);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		String id="", first="", last="";
		while(rs.hasNext()) {
			QuerySolution sol = rs.next();
			
//			System.out.println(sol.get("?id") + " " + sol.get("?first") + " " +sol.get("?last") + " " );
			id = ""+sol.get("?id");
			first = ""+sol.get("?first");
			last = ""+sol.get("?last");
			
			subject = model2.createResource("https://fake.data/Person-a"+id);
			predicate = RDF.type;  
			model2.add(subject, predicate, mainPersonSubject);
			
			predicate = model2.createProperty("https://fake.model/hasId");
			object = model2.createLiteral("a"+id);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasName");
			object = model2.createLiteral(first+" "+last);
			model2.add(subject, predicate, object);
			
			subject = null;
			predicate = null;
			object = null;
			id = null;
			first = null;
			last = null;
			
			i++;
		}
		qexec.close();
		qexec = null;
		
		
		subject = null;
		predicate = null;
		object = null;
		
		
		
		
		// Filling directors first
		query = QueryFactory.create("Select ?id ?first ?last "
				+ "where { "
				+ 	" ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Directors.data/Directors> ."
				+ 	" ?d <https://fake.model/hasId> ?id ."
				+ 	" ?d <https://fake.model/hasFirst> ?first ."
				+ 	" ?d <https://fake.model/hasLast> ?last ." 
				+ "}");   
		
		qexec = QueryExecutionFactory.create(query, model1);
		rs = qexec.execSelect();
		i = 0;
		while(rs.hasNext()) {
			QuerySolution sol = rs.next();
			
			id = ""+sol.get("?id");
			first = ""+sol.get("?first");
			last = ""+sol.get("?last");
			
			subject = model2.createResource("https://fake.data/Person-d"+id);
			predicate = RDF.type;  
			model2.add(subject, predicate, mainPersonSubject);
			
			predicate = model2.createProperty("https://fake.model/hasId");
			object = model2.createLiteral("d"+id);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasName");
			object = model2.createLiteral(first+" "+last);
			model2.add(subject, predicate, object);
			
			subject = null;
			predicate = null;
			object = null;
			id = null;
			first = null;
			last = null;
			
			i++;
		}
		qexec.close();
		qexec = null;
		
		System.out.println("Person done");
	}
	
	
	public static void createActors(Model model1, Model model2) {
		
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		Resource mainActorSubject = model2.createResource("<https://Actors.data/Actors>");
		
		Query query = QueryFactory.create("Select ?mid ?aid "
				+ "where { "
				+ 	" ?m <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Movies.data/Movies> ."
				+ 	" ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Actors.data/Actors> ."
				+ 	" ?m <https://fake.data/hasActor> ?a ."
				+ 	" ?a <https://fake.model/hasId> ?aid ."
				+ 	" ?m <https://fake.model/hasId> ?mid ." 
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model1);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		String mid="", aid="";
		while(rs.hasNext()) {
			QuerySolution sol = rs.next();
			mid = ""+sol.get("?mid");
			aid = ""+sol.get("?aid");
			
			subject = model2.createResource("https://fake.data/Actor-a"+aid);
			predicate = RDF.type;  
			model2.add(subject, predicate, mainActorSubject);
			
			predicate = model2.createProperty("https://fake.model/hasPersonId");
			object = model2.createLiteral("a"+aid);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasMovieId");
			object = model2.createLiteral(mid);
			model2.add(subject, predicate, object);
			 
			predicate = model2.createProperty("https://fake.data/hasPerson");
			object = model2.createResource("https://fake.data/Person-a"+aid);
			model2.add(subject, predicate, object); 
			
			predicate = model2.createProperty("https://fake.data/hasActor");
//			Resource movie = model2.createResource("https://fake.data/Movie-"+mid);
			model2.add( model2.createResource("https://fake.data/Movie-"+mid), predicate, subject); 
			
			subject = null;
			predicate = null;
			object = null;
			
			i++;
		}
		qexec.close();
		qexec = null;
		System.out.println("Actors done");		
	}
	
	public static void createDirectors(Model model1, Model model2) {
		
		Resource subject = null;
		Property predicate = null;
		RDFNode object = null;
		
		Resource mainDirectorSubject = model2.createResource("<https://Directors.data/Directors>");
		
		Query query = QueryFactory.create("Select ?mid ?did "
				+ "where { "
				+ 	" ?m <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Movies.data/Movies> ."
				+ 	" ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://Directors.data/Directors> ."
				+ 	" ?m <https://fake.data/hasActor> ?d ."
				+ 	" ?d <https://fake.model/hasId> ?did ."
				+ 	" ?m <https://fake.model/hasId> ?mid ."  
				+ "}");   
		
		QueryExecution qexec = QueryExecutionFactory.create(query, model1);
		ResultSet rs = qexec.execSelect();
		int i = 0;
		String mid="", did="";
		while(rs.hasNext()) {
			QuerySolution sol = rs.next();
			mid = ""+sol.get("?mid");
			did = ""+sol.get("?did");
			
			subject = model2.createResource("https://fake.data/Director-d"+did);
			predicate = RDF.type;  
			model2.add(subject, predicate, mainDirectorSubject);
			
			predicate = model2.createProperty("https://fake.model/hasPersonId");
			object = model2.createLiteral("d"+did);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.model/hasMovieId");
			object = model2.createLiteral(mid);
			model2.add(subject, predicate, object);
			
			predicate = model2.createProperty("https://fake.data/hasPerson");
			object = model2.createResource("https://fake.data/Person-d"+did);
			model2.add(subject, predicate, object); 
			
			predicate = model2.createProperty("https://fake.data/hasDirector");
			model2.add( model2.createResource("https://fake.data/Movie-"+mid), predicate, subject);
			
			subject = null;
			predicate = null;
			object = null;
			
			i++; 
		}
		qexec.close();
		qexec = null;
		System.out.println("Directors done");		 
	}
	
}
