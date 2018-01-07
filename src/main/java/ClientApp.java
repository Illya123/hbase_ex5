import java.util.List;

/**
 * Created by illya on 07.01.18.
 */
public class ClientApp
{


    public static void main(String[] args)
    {
        final String TABLE_NAME= "actors";
        final String COLUMN_FAMILY = "films";
        final String COLUMN_NAME = "role";

        HBaseClient client = new HBaseClient();

        List<ActorInfo> aInfoList = TSVParseHelper.readTSV();
        client.deleteTable(TABLE_NAME);
        client.createTable(TABLE_NAME, COLUMN_FAMILY);
        client.createTestData(TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME, aInfoList);

        long roleCount = client.getMaxRoleCount(TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME);
        List<String> actors = client.getActorByRoles(TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME, roleCount);

        for ( String actor : actors)
        {
            System.out.println("Actor participated in " + roleCount + " films!");
        }
    }
}
