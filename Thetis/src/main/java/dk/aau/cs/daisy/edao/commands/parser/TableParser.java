package dk.aau.cs.daisy.edao.commands.parser;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.Utils;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableParser
{
    public static Table<String> toTable(File f)
    {
        try
        {
            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(f.toPath());
            Type type = new TypeToken<HashMap<String, List<List<String>>>>(){}.getType();
            Map<String, List<List<String>>> map = gson.fromJson(reader, type);

            return new DynamicTable<>(map.get("queries"));
        }

        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static Table<String> toTable(List<List<String>> matrix)
    {
        return new DynamicTable<>(matrix);
    }

    public static JsonTable parse(Path path)
    {
        JsonTable table = Utils.getTableFromPath(path);
        return table == null || table._id  == null || table.rows == null ? null : table;
    }

    public static JsonTable parse(File file)
    {
        return parse(file.toPath());
    }
}
