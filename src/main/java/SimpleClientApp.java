import java.util.Map;

/**
 * Created by illya on 07.01.18.
 */
public class SimpleClientApp
{

    public static void main(String[] args)
    {
        final String TABLE_NAME= "test";
        final String COLUMN_FAMILY = "users";
        final String COLUMN_NAME = "name";
        Map<String, String> results;
        HBaseSimpleClient client = new HBaseSimpleClient();

        client.createTable(TABLE_NAME, COLUMN_FAMILY);
        client.createTestData(TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME);
        results = client.getTableEntries(TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME);
        printMap(results);
        client.deleteTable(TABLE_NAME);
    }

    static void printMap(Map<String, String> map)
    {
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            System.out.println("key: " + entry.getKey() + "  value: " + entry.getValue());
        }
    }
}

