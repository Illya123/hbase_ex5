import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by illya on 07.01.18.
 */
public class TSVParseHelper
{
    private static final String tsvPath = TSVParseHelper.class
            .getClassLoader()
            .getResource("performance.tsv")
            .getPath();

    static List<ActorInfo> readTSV()
    {
        List<ActorInfo> entries = new ArrayList<ActorInfo>();

        TsvParserSettings settings = new TsvParserSettings();
        // for Windows "\r\n", MacOS "\r", Linux "\n"
        settings.getFormat().setLineSeparator("\n");

        TsvParser parser = new TsvParser(settings);

        List<String[]> rows = parser.parseAll(new File( tsvPath ));

        for (String [] row : rows)
        {
            entries.add(new ActorInfo(row[2], row[3], row[5]));
        }

        return entries;
    }
}
