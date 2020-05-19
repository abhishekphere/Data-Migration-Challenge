import java.sql.*;  

public class transaction {
	public static void main(String[] args) {
		
		String genres[] = {"Scary", "funny", "Romantic", "Thriller", "Animated", "Adventure film", "Crime film", "Documentry film", "Superhero movie", "Black Comedy" };
		Connection connect = null;
		try{  
			Class.forName("com.mysql.jdbc.Driver");  
			connect = DriverManager.getConnection( "jdbc:mysql://localhost:3306/imdb_dump","username","password");    
			Statement statement = connect.createStatement();  
			
			connect.setAutoCommit(false);
			int j = 161;
			for(int i = 0; i < 10; i++) {
				if(i == 2) {
					throw new SQLException(); 
				} 
				 
				String query = "Insert into genres values("+Integer.toString(j)+", '"+genres[i].toString()+"')";
				statement.executeUpdate(query);
				
				j++;
			}
			connect.commit();
			connect.close();  
		} 
		catch(Exception e){   
			
            try {
            	System.err.print("Transaction is being rolled back");
				connect.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}  
		
	}
}
