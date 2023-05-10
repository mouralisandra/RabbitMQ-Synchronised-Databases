import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;


public class ManageData {
    public final static String HO_QUEUE = "ho_queue";
    public final static String HO_DBNAME = "HO";

    public static void createDb(String dbName){
        try{
            java.sql.Connection con= DriverManager.getConnection("jdbc:mysql://localhost/?user=root&password=");
            Statement stmt=con.createStatement();
            Boolean result = stmt.execute("CREATE DATABASE if not exists "+dbName);
            con.close();
        }catch( Exception e){
            System.out.println("error in creating data base "+ dbName);
            System.out.println(e);
        }
    }
    public static void createProductTable(String dbName){
        try{
            java.sql.Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/"+dbName,"root","");
            Statement stmt=con.createStatement();

            String sql = "CREATE TABLE IF NOT EXISTS product (" +
                    "id VARCHAR(100)," +
                    "product VARCHAR(50)," +
                    "qty INT," +
                    "cost FLOAT," +
                    "amt FLOAT," +
                    "tax FLOAT," +
                    "total FLOAT," +
                    "region VARCHAR(50)" +
                    ")";



            stmt.executeUpdate(sql);
            con.close();
        }catch(Exception e){
            System.out.println("error in creating the product table in database "+ dbName);
            System.out.println(e);
        }
    }

    public static void sendToDB(Product p, String dbName){
        try{
            java.sql.Connection con= DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/"+dbName,"root","");

            String sql = "replace into product (id, product, qty, cost, amt, tax, total, region) values (?, ?, ?, ?, ?, ?, ?, ?);";
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setString(1, p.id);
            stmt.setString(2, p.product);
            stmt.setInt(3, p.qty);
            stmt.setFloat(4, p.cost);
            stmt.setFloat(5, p.amt);
            stmt.setFloat(6, p.tax);
            stmt.setFloat(7, p.total);
            stmt.setString(8, p.region);
            stmt.executeUpdate();
            con.close();
        }catch(Exception e){
            System.out.println("failed to send the product: " + p.product );
            System.out.println(e);
        }
    }

    public static String[][] getAllProducts(String dbName){
        String[][] products = new String[100][9];
        int i=0;
        try {
            java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbName, "root", "");
            Statement stmt = con.createStatement();
            String sql = "SELECT * FROM product;";
            ResultSet rs =  stmt.executeQuery(sql);
            while (rs.next()) {
                products[i][0] = rs.getString(1);
                products[i][1] = rs.getString(2);
                products[i][2] = Integer.toString(rs.getInt(3));
                products[i][3] = Float.toString(rs.getFloat(4));
                products[i][4] = Float.toString(rs.getFloat(5));
                products[i][5] = Float.toString(rs.getFloat(6));
                products[i][6] = Float.toString(rs.getFloat(7));
                products[i][7] = rs.getString(8);
                i++;
            }

            
        }catch (Exception e){
            System.out.println("Error in retrieving data from "+ dbName );
        }
        String[][] products1 = new String[i][9];
        for(int j=0 ; j<i; j++){
            products1[j][0] = products[j][0];
            products1[j][1] = products[j][1];
            products1[j][2] = products[j][2];
            products1[j][3] = products[j][3];
            products1[j][4] = products[j][4];
            products1[j][5] = products[j][5];
            products1[j][6] = products[j][6];
            products1[j][7] = products[j][7];
        }
        return products1;
    }

    public static void sendToHO(Product p) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(HO_QUEUE, false, false, false, null);
            String msg = "ADD"+p.toString();
            channel.basicPublish("", HO_QUEUE, null, msg.getBytes());
        }catch (Exception e){
            System.out.println("error sending data to HO ");
        }
    }

    public static void sendQueryToHO(String sql) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(HO_QUEUE, false, false, false, null);
            String msg = sql.toString();
            channel.basicPublish("", HO_QUEUE, null, msg.getBytes());
        }catch (Exception e){
            System.out.println("error sending data to HO ");
        }
    }

    public static void sendOldDataToHO(String dbName)  {
        try {
                    java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbName, "root", "root");
                    Statement stmt = con.createStatement();
                    String sql = "SELECT * FROM product;";
                    ResultSet product =  stmt.executeQuery(sql);
                    while (product.next()){
                        String id = product.getString(1);
                        String productname = product.getString(2);
                        float qty = product.getInt(3);
                        float cost = product.getFloat(4);
                        float amt = product.getFloat(5);
                        float tax = product.getFloat(6);
                        float total = product.getFloat( 7);
                        String region = product.getString(8);
                Product p = new Product(id,productname, (int) qty, cost, amt, tax, total, region);
                sendToHO(p);
            }
            

        }catch (Exception e){
        }
    }

    public static void execQuery(String sql,String dbName){
        try{
            java.sql.Connection con= DriverManager.getConnection("jdbc:mysql://localhost:3306/"+dbName,"root","");
            Statement stmt=con.createStatement();
            stmt.executeUpdate(sql);
            con.close();
        }catch(Exception e){
            System.out.println("error in executing  query : "+ sql);
        }
    }

    public static  void removeFromBD(String id, String dbName){
            String sql = "DELETE FROM product WHERE id = '"+id+"';";
            execQuery(sql,dbName);
    }

    public static void removeFromHO(String id){
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(HO_QUEUE, false, false, false, null);
            channel.basicPublish("", HO_QUEUE, null, ("DEL"+id).getBytes());
        }catch (Exception e){
            System.out.println("error sending data to HO ");
        }
    }


}
