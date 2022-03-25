
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.table.DefaultTableModel;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author David Iruafeimi
 * 
 */
public class DBEngine {
    
    private Connection connect = DBEngine.runConnect();
    private PreparedStatement preState;
    private ResultSet reSet;
    public static Connection runConnect(){
        try {
//            Class.forName("org.apache.derby.jdbc.ClientDriver");
//            Connection con = DriverManager.getConnection("jdbc:derby://localhost/smallmarket","shop","shop");
            
            //Internal database connection 
             Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            Connection con = DriverManager.getConnection("jdbc:derby:smallmarket","shop","shop");
            return con;
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(DBEngine.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    /**
     * @param tableName Name of the table
     * @param columns Columns to select from
     * @param where Where clause to use
     * @param order Order clause to use
     * @return 
     * @throws java.lang.Exception    
     */
    public ResultSet runSelect(String tableName, String columns, String where, String order) throws Exception{
        
        String query ="SELECT "+columns+" FROM "+tableName;
        if(!where.isEmpty()){
            query = query + " WHERE "+where+" ";
        }
        if(!order.isEmpty()){
            query = query + order;
        }
        
        preState = connect.prepareStatement(query);
        reSet = preState.executeQuery();
        
        return reSet;
    }  
    
    public ResultSet runSelect(String tableName, String columns, String where, String order, String group) throws Exception{
        
        String query ="SELECT "+columns+" FROM "+tableName;
        if(!where.isEmpty()){
            query = query + " WHERE "+where+" ";
        }
        if(!order.isEmpty()){
            query = query + order;
        }
        if(!group.isEmpty()){
            query = query +group;
        }
        
        preState = connect.prepareStatement(query);
        reSet = preState.executeQuery();
        
        return reSet;
    }
    /**
     * 
     * @param tableName The name of the table
     * @param columns Array of the columns
     * @param rows Array of the rows
     * @return
     * @throws Exception 
     */
    public boolean runInsert(String tableName, String columns[], String rows[]) throws Exception{
        boolean check;
        //String query = "INSERT INTO "+tableName+"("+columns+") VALUES("+rows+");";
        
        String query = "INSERT INTO "+tableName+"(";
        
        for(int i = 0; i<columns.length; i++){
            if(i == (columns.length)-1){
                query = query + columns[i];
            }else{
                query = query + columns[i]+",";
            }
            
        }
        query = query+")  VALUES(";
        
        for(int i = 0; i<rows.length; i++){
            if(i==(rows.length)-1){
                query = query +"\'"+rows[i]+"\'";
            }else{
                query = query +"\'"+rows[i]+"\',";
            }
        }
        query = query + ")";
        
        preState = connect.prepareStatement(query);
        check = preState.execute();
        
        if(check){
            return true;
        }else{
            return false;
        }
    }
    /**
     * 
     * @param tableName Name of the table
     * @param columns Array of columns
     * @param values Array of the values
     * @return
     * @throws Exception 
     */
    public boolean runUpdate(String tableName, String columns[], String values[], String where) throws Exception{
        
        String query = "UPDATE "+tableName+" SET ";
        for(int x= 0; x<columns.length; x++){
            if(x==(columns.length)-1){
                query = query + columns[x] + " = '"+values[x]+"'";
            }else{
                query = query + columns[x] + " = '"+values[x]+"',";
            }
        }
        
        query = query + " WHERE "+where;
        preState = connect.prepareStatement(query);
        boolean ck = preState.execute();
        preState.close();

        if(ck){
            return true;
        }else{
            return false;
        }
    }       
    /**
     * 
     * @param tableName Name of the table
     * @param where where clause
     * @return 
     * @throws Exception 
     */
    public boolean runDelete(String tableName, String where) throws Exception{
        
        String query = "DELETE FROM "+tableName+" WHERE "+where;
        preState = connect.prepareStatement(query);
        boolean ck = preState.execute();
        preState.close();

        if(ck){
            return true;
        }else{
            return false;
        }
    }    
    /**
     * 
     * @param rs ResultSet of the tables
     * @param columns Arrays of the columns
     * @param colTitle Array title of columns
     * @return
     * @throws Exception 
     */
    public DefaultTableModel fillTable(ResultSet rs, String columns[], String colTitle[]) throws Exception{
        
        DefaultTableModel model = new DefaultTableModel(){
            @Override
            public boolean isCellEditable(int row, int column){
                return false;
            }
        };
        
      
        for(int i = 0; i<colTitle.length; i++){
            model.addColumn(colTitle[i]);
        }
  
        
        
        while(rs.next()){
            ArrayList row = new ArrayList();
            for(int j=0; j<columns.length; j++){
                row.add(rs.getString(columns[j]));
            }
            
            model.addRow(row.toArray(new String[row.size()]));
        }
      //  rs.close();
        model.fireTableDataChanged();
        return model;
    }
    
    public DefaultTableModel fillTable(ResultSet rs, int columns[], String colTitle[]) throws Exception{
        
        DefaultTableModel model = new DefaultTableModel(){
            @Override
            public boolean isCellEditable(int row, int column){
                return false;
            }
        };
        
      
        for(int i = 0; i<colTitle.length; i++){
            model.addColumn(colTitle[i]);
        }
  
        
        
        while(rs.next()){
            ArrayList row = new ArrayList();
            for(int j=0; j<columns.length; j++){
                row.add(rs.getInt(columns[j]));
            }
            
            model.addRow(row.toArray(new String[row.size()]));
        }
      //  rs.close();
        model.fireTableDataChanged();
        return model;
    }
    
    public boolean runTruncate(String tableName) throws Exception{
        String tbName = tableName;
        String query = "TRUNCATE TABLE "+tbName;
        PreparedStatement stat = connect.prepareStatement(query);
        stat.execute();
        return true;
    }
    
    void createDB() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
