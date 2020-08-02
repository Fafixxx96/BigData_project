import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HBaseClient {


    private static final String PATH_Q1 = "D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query1/out/out12";
    private static final String PATH_Q2 = "D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query2/out/out2";
    private static final String PATH_Q3 = "D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query3/out/out3";
    private static final String PATH_Q4 = "D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query4/out/out4";

    /* Configuration Parameters */
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER  = "localhost:60000";
    private static final int    HBASE_MAX_VERSIONS = Integer.MAX_VALUE;

    private enum ALTER_COLUMN_FAMILY {
        ADD,
        DELETE
    }

    private static Connection connection = null;

    private static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    private static Connection getConnection() throws IOException, ServiceException {

        if (!(connection == null || connection.isClosed() || connection.isAborted()))
            return connection;

        Logger.getRootLogger().setLevel(Level.ERROR);

        Configuration conf  = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
        conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        conf.set("hbase.master", HBASE_MASTER);

        /* Check configuration */
        HBaseAdmin.checkHBaseAvailable(conf);


            System.out.println("HBase is running!");

        connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    private static boolean createTable(String tableName, String... columnFamilies)  throws IOException, ServiceException{



            Admin admin = getConnection().getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));


            for (String columnFamily : columnFamilies) {
                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);
                cd.setMaxVersions(HBASE_MAX_VERSIONS);
                tableDescriptor.addFamily(cd);
            }

            if(!admin.tableExists(tableDescriptor.getTableName())) admin.createTable(tableDescriptor);
            return true;
    }

    private boolean alterColumnFamily(ALTER_COLUMN_FAMILY operation, String table, String columnFamily)  throws IOException, ServiceException{


            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);


            switch (operation){
                case ADD:
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    admin.addColumn(tableName, columnDescriptor);
                    break;
                case DELETE:
                    admin.deleteColumn(tableName, Bytes.toBytes(columnFamily));
                    break;
                default:
                    return false;
            }

            return true;


    }

    private static boolean put(String table, String rowKey, String... columns)  throws IOException, ServiceException {

        if (columns == null || (columns.length % 3 != 0)) {
            // Invalid usage of the function; columns should contain 3-ple in the
            // following format:
            // - columnFamily
            // - column
            // - value
            return false;
        }

        Table hTable = getConnection().getTable(TableName.valueOf(table));

        Put p = new Put(b(rowKey));

        for (int i = 0; i < (columns.length / 3); i++) {

            String columnFamily = columns[i * 3];
            String column = columns[i * 3 + 1];
            String value = columns[i * 3 + 2];

            p.addColumn(b(columnFamily), b(column), b(value));

        }

        // Saving the put Instance to the HTable.
        hTable.put(p);

        // closing HTable
        hTable.close();

        return true;
    }

    private String get(String table, String rowKey, String columnFamily, String column) throws IOException, ServiceException{

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));



            Get g = new Get(b(rowKey));

            // Narrowing the scope
            // g.addFamily(b(columnFamily));
            // g.addColumn(b(columnFamily), b(column));

            // Reading the data
            Result result = hTable.get(g);

            byte [] value = result.getValue(b(columnFamily), b(column));
            return Bytes.toString(value);

    }

    private static void scanTable(String table, String columnFamily, String column) throws IOException, ServiceException {

        Table products = getConnection().getTable(TableName.valueOf(table));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null)
            scan.addFamily(b(columnFamily));

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            System.out.println("Found row : " + result);
        }

        scanner.close();

    }

    private static File[] takeFiles(String path){
        File folder = new File(path);
        List<File> files = new ArrayList<File>();
        for(File f: folder.listFiles())
            if(!f.getName().contains(".crc")) files.add(f);
        return files.toArray(new File[files.size()]);
    }

    private static List<String[]> loadFile (File[] files, String separator) throws IOException {
        List<String[]> rows = new ArrayList<String[]>();
        String s[];
        BufferedReader br;
        for(int i = 0; i<files.length; i++) {
            br = new BufferedReader(new FileReader(files[i]));
            String line;
            for (; ; ) {
                line = br.readLine();
                if (line==null) break;
                s = line.split(separator);
                rows.add(s);
            }
        }
        return rows;
    }

    private static void storeQuery1() throws IOException, ServiceException {

        List<String[]> rows = loadFile(takeFiles(PATH_Q1), "/");

        createTable("query1", "result");

        for(String[] s: rows){
            put("query1", s[0].trim(),  "result","azienda", s[1].trim() );
            put("query1", s[0].trim(), "result", "quantita", s[2].trim() );

        }
    }

    private static void storeQuery3() throws IOException, ServiceException {

        List<String[]> rows = loadFile(takeFiles(PATH_Q3), ",");

        createTable("query3", "result");

        for(String[] s: rows){
            String rowKey = s[0].trim().substring(1); //regione
            put("query3", rowKey,  "result","hospital", s[1].trim().substring(1) );
            put("query3", rowKey, "result", "intensive", s[2].trim() );
            put("query3", rowKey, "result", "home", s[3].trim() );
            put("query3", rowKey, "result", "newPositive", s[4].trim() );
            put("query3", rowKey, "result", "death", s[5].trim() );
            put("query3", rowKey, "result", "recovered", s[6].trim().substring(0, s[6].length()-2));
        }
    }

    private static void storeQuery2() throws IOException, ServiceException {

        List<String[]> rows = loadFile(takeFiles(PATH_Q2), "/");

        createTable("query2", "result");

        for(String[] s: rows){
            put("query2", s[0].trim(),  "result", s[1].trim(), s[2].trim());
        }
    }

    private static void storeQuery4() throws IOException, ServiceException {

        List<String[]> rows = loadFile(takeFiles(PATH_Q4), ",");

        createTable("query4", "result");

        for(String[] s: rows){
            String rowKey = s[0].trim().substring(1); //regione
            put("query4", rowKey,  "result","age", s[1].trim().substring(1) );
            put("query4", rowKey, "result", "intubed", s[2].trim() );
            put("query4", rowKey, "result", "pneumonia", s[3].trim() );
            put("query4", rowKey, "result", "diabet", s[4].trim() );
            put("query4", rowKey, "result", "Bpco", s[5].trim() );
            put("query4", rowKey, "result", "others", s[6].trim() );
            put("query4", rowKey, "result", "cardio", s[7].trim() );
            put("query4", rowKey, "result", "obesity", s[8].trim() );
            put("query4", rowKey, "result", "renal", s[9].trim() );
            put("query4", rowKey, "result", "tobacco", s[10].trim() );
            if(rowKey.equals("female")) put("query4", rowKey, "result", "pregnant", s[11].trim().substring(0, s[11].length()-2) );
        }
    }

    private static void retrieveQuery1() throws IOException, ServiceException {
        scanTable("query1", "result", "azienda");
        scanTable("query1", "result", "quantita");
    }

    private static void retrieveQuery2() throws IOException, ServiceException {
        scanTable("query2", "result", "CS");


    }

    public static void main(String[] args) throws IOException, ServiceException {

        storeQuery1();

        storeQuery2();

        storeQuery3();

        storeQuery4();

        System.out.println("HBase loaded");

        //retrieveQuery1();

        //retrieveQuery2();


    }
}
