import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by illya on 07.01.18.
 */
public class HBaseClient extends HBaseSimpleClient
{
    private static final String COPROSECCOR_CLASS_NAME = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

    private AggregationClient agClient;

    {
        agClient = new AggregationClient(super.getConfiguration());
    }

    public void createTable(String tableName, String columnFamily)
    {
        try
        {
            if (!super.getAdmin().tableExists(TableName.valueOf(tableName)))
            {
                HTableDescriptor hTDec = new HTableDescriptor(TableName.valueOf(tableName));
                hTDec.addFamily(new HColumnDescriptor(columnFamily));
                hTDec.addCoprocessor(COPROSECCOR_CLASS_NAME);
                super.getAdmin().createTable(hTDec);

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

    public void createTestData(String tableName, String columFamily, String columnName, List<ActorInfo> aInfos)
    {
        List<Put> puts = new ArrayList<Put>();
        Table actorsTable = null;

        try
        {
            if (super.getAdmin().tableExists(TableName.valueOf(tableName)))
            {
                actorsTable = super.getConnection().getTable(TableName.valueOf(tableName));
            }
            else
            {
                System.out.println("Table with name \"" + tableName + "\" does not exist!");
                //throw new Exception("Table with name " + tableName + " does not exist!")
            }

            System.out.println("Writing data to table ...");

            for ( ActorInfo aInfo : aInfos)
            {
                Put put = new Put(aInfo.getActorName().getBytes());
                put.addColumn(columFamily.getBytes(), aInfo.getFilm().getBytes(), aInfo.getRole().getBytes());
                actorsTable.incrementColumnValue(aInfo.getActorName().getBytes(), columFamily.getBytes(), columnName.getBytes(), 1);

                puts.add(put);
            }

            actorsTable.put(puts);

            System.out.println("Writing data to table completed!");

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public int roleCountActor(String tableName, String columnFamily, String columnName, String actor)
    {
        Table table = null;
        int result = -1;

        try
        {
            if (super.getAdmin().tableExists(TableName.valueOf(tableName)))
            {
                table = super.getConnection().getTable(TableName.valueOf(tableName));
            }
            else
            {
                System.out.println("Table with name \"" + tableName + "\" does not exist!");
                //throw new Exception("Table with name " + tableName + " does not exist!")
            }

            Result get = table.get(new Get(Bytes.toBytes(actor)));
            result = Bytes.toInt(get.getValue(columnFamily.getBytes(), columnName.getBytes()));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return result;
    }

    public long getMaxRoleCount(String tableName, String columnFamily, String columnName)
    {
        Scan scan = new Scan();
        scan.addColumn(columnFamily.getBytes(), columnName.getBytes());

        long count = 0;

        try
        {
            count = agClient.max(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        }
        catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }

        return count;
    }

    public List<String> getActorByRoles(String tableName, String columnFamily, String columnName, long value)
    {
        Table table;
        List<String> results = new ArrayList<String>();
        ResultScanner scanner;
        Scan scan = new Scan();


        SingleColumnValueFilter filter = new SingleColumnValueFilter(columnFamily.getBytes(), columnName.getBytes(), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        scan.setFilter(filter);

        try
        {
            table = super.getConnection().getTable(TableName.valueOf(tableName));
            scanner = table.getScanner(scan);

            for (Result row : scanner)
            {
                String rowKey = row.getRow().toString();
                results.add(rowKey);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return results;
    }
}