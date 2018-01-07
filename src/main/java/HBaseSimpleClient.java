import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by illya on 06.01.18.
 */
public class HBaseSimpleClient
{
    private final String COLUMN_FAMILY = "users";
    private final String COLUMN_NAME = "name";
    private final String[] INPUT = {"Donald", "Daisy", "Goofy"};


    private final String confFilePath = this.getClass()
            .getClassLoader()
            .getResource("hbase-site.xml")
            .getPath();

    private Configuration config;
    private Connection connection;
    private Admin admin;

    {
        config = HBaseConfiguration.create();
        config.addResource(confFilePath);


        try
        {
            HBaseAdmin.checkHBaseAvailable(config);
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            System.out.println("Connected to HBase!");
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        catch (ServiceException e)
        {
            e.printStackTrace();
        }

    }

    public Configuration getConfiguration()
    {
        return config;
    }

    public Connection getConnection()
    {
        return connection;
    }

    public Admin getAdmin()
    {
        return admin;
    }

    public void createTable(String tableName, String columnFamily)
    {
        try
        {
            if (!admin.tableExists(TableName.valueOf(tableName)))
            {
                HTableDescriptor hTDec = new HTableDescriptor(TableName.valueOf(tableName));
                hTDec.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(hTDec);

                System.out.println("Table " + tableName + " was created!");
            }
            else
            {
                System.out.println("Table with name \"" + tableName + "\" already exists!");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName)
    {
        Table table;

        try
        {
            if (admin.tableExists(TableName.valueOf(tableName)))
            {
                table = connection.getTable(TableName.valueOf(tableName));

                admin.disableTable(table.getName());
                admin.deleteTable(table.getName());

                System.out.println("Table with name \"" + tableName + "\" was deleted!");
            }
            else
            {
                System.out.println("Table with name \"" + tableName + "\" does not exist!");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    void createTestData(String tableName, String columFamily, String columnName) {

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            System.out.println("Writing values to " + tableName + " ...");
            for (int i = 0; i < INPUT.length; i++) {
                String rowKey = "row" + i;

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(columFamily.getBytes(), columnName.getBytes(), INPUT[i].getBytes());
                table.put(put);
            }
            System.out.println("Done writing!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  String getResultsByRowKey(String tableName, String columnFamily, String columnName, String rowKey)
    {
        Table table;
        String result = "";

        try
        {
            if (!admin.tableExists(TableName.valueOf(tableName)))
            {
                table = connection.getTable(TableName.valueOf(tableName));
                Result get = table.get(new Get(rowKey.getBytes()));
                result = Bytes.toString(get.getValue(columnFamily.getBytes(), columnName.getBytes()));
                System.out.println("Table with name \"" + tableName + "\" was deleted!");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return result;
    }

    public Map<String, String> getTableEntries(String tableName, String columnFamily, String columnName)
    {
        Table table;
        ResultScanner scanner;
        Map<String, String> results = new HashMap<String, String>();
        Scan scan = new Scan();

        try
        {
            if(!admin.tableExists(TableName.valueOf(tableName)))
            {
                System.err.println("Table does not exist!");
                return null;
            }
            else
            {
                table = connection.getTable(TableName.valueOf(tableName));
                scanner = table.getScanner(scan);

                for (Result row : scanner)
                {
                    String rowKey = Bytes.toString(row.getRow());
                    String value = Bytes.toString(row.getValue(columnFamily.getBytes(), columnName.getBytes()));

                    results.put(rowKey, value);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return results;
    }
}
