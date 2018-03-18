package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class From_Visitor {
    public static String table_name = null;
    public static void ret_type(FromItem stmt)
    {
        if(stmt instanceof SubJoin)
        {
            SubJoin subJoin = (SubJoin) stmt;
            Join join = subJoin.getJoin();
//            System.out.println(join.isSimple());
        }
        else if(stmt instanceof SubSelect)
        {
            SubSelect subSelect = (SubSelect) stmt;
            Select_Visitor.ret_type(subSelect.getSelectBody());
            Iterator it = Data_Storage.selectedColumns.keySet().iterator();
            Data_Storage.from_alias = subSelect.getAlias();
            if(subSelect.getAlias()!=null) {
                while (it.hasNext()) {
                    String temp_it = it.next().toString();
                    if (temp_it.indexOf(".") != -1) {
                        StringTokenizer str_tok = new StringTokenizer(temp_it, ".");
                        String tablename = str_tok.nextElement().toString();
                        String col_name = str_tok.nextElement().toString();
//                    Data_Storage.alias_table.put(new StringBuilder(stmt.getAlias()).append(".").append(col_name).toString(),te;mp_it);
                        if (Data_Storage.table_alias.containsKey(tablename))
                            tablename = Data_Storage.table_alias.get(tablename);
                        Iterator iterate = Data_Storage.tables.get(tablename).keySet().iterator();
                        while (iterate.hasNext()) {
                            String col_name_temp = iterate.next().toString();
                            Data_Storage.alias_table.put(subSelect.getAlias().concat(".").concat(col_name_temp), tablename.concat(".").concat(col_name_temp));
                        }
                    }
                }
            }
        }
        else if(stmt instanceof Table)
        {
                Table table = (Table) stmt;
                table_name = table.getName();
                if(table.getAlias()!=null)
                {
                    Data_Storage.table_alias.put(table.getAlias(),table_name);
                }
                Data_Storage.oper = new FileIterator_Interface(table_name);
                ArrayList<String> cols = new ArrayList<>(Data_Storage.tables.get(table_name).keySet());
                Iterator it = cols.iterator();
                while(it.hasNext()){
                    Data_Storage.current_schema.put(it.next().toString(),table_name);
                }
                Data_Storage.project_columns.put(table_name,new ArrayList<String>());
        }
    }

}
