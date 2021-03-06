package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.io.*;
import java.util.*;

public class FileIterator_Interface implements Iterator_Interface{
    BufferedReader read;
    public String new_file;

    public String getNew_file() {
        return new_file;
    }

    public void setNew_file(String new_file) {
        this.new_file = new_file;
    }

    String aliastableName;
    HashMap<String, ArrayList<Column>> schemaMap = new HashMap<>();
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> inSchema = new LinkedHashMap<>();

    boolean filetype ;

    public FileIterator_Interface(String new_file,String aliastableName,boolean filetype) {
        this.new_file = new_file;
        if(aliastableName == null){
            this.aliastableName = new_file;
        }else{
            this.aliastableName = aliastableName;
        }
        this.filetype = filetype;
        String file;
        if(filetype)
            file = "data/"+new_file+".dat";
        else
            file = "indexes/"+new_file+".txt";
        try {
            read = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(){
        generateSchema();
    }

    @Override
    public PrimitiveValue[] readOneTuple() {
        if (read == null) {
            return null;
        }
        String new_line = null;
        try {
            new_line = read.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (new_line == null) {
            return null;
        }

        PrimitiveValue[] str_split = new PrimitiveValue[this.outSchema.size()];
        ArrayList<String> str = new ArrayList<>();
        StringTokenizer str_tok = new StringTokenizer(new_line,"|");
        while (str_tok.hasMoreElements()){
            str.add(str_tok.nextElement().toString());
        }
        int i = 0;
        Iterator it = this.inSchema.values().iterator();
        while(it.hasNext()){
            Schema s = (Schema) it.next();
            String dataType = s.getDataType();
            switch (dataType){
                case "INTEGER":
                    str_split[i] = new LongValue(str.get(s.getPosition()));
                    break;
                case "DOUBLE":
                    str_split[i] = new DoubleValue(str.get(s.getPosition()));
                    break;
                case "DATE":
                    str_split[i] = new DateValue(str.get(s.getPosition()));
                    break;
                default:
                    str_split[i] = new StringValue(str.get(s.getPosition()));
                    break;
            }
            i++;
        }

            // add elements to the array

//        while(str_tok.hasMoreElements())
//        {
//            str_split.add(str_tok.nextElement().toString());
//        }
//        Iterator it = Data_Storage.tables.get(new_file).keySet().iterator();
//        Iterator iter_string = str_split.iterator();
//        String col_name;
//        ArrayList<String> to_send = new ArrayList<>();
////        if(Data_Storage.table_alias.containsValue(new_file))
////        {
////
////          Set set = Data_Storage.table_alias.entrySet();
////          Iterator set_iterator  = set.iterator();
////          while(set_iterator.hasNext())
////          {
////              Map.Entry entry =(Map.Entry) set_iterator.next();
////              if(entry.getValue().equals(new_file))
////                  new_file = entry.getKey().toString();
////          }
////        }
////        if(aliastableName==null)
////            aliastableName = new_file;
////
////        if(schemaMap.containsKey(new_file)){
////            schema = null;
////        }else{
////            while(it.hasNext()){
////                col_name = it.next().toString();
////                Column col = new Column(new Table(aliastableName), col_name);
////                schema.add(col);
////            }
////            schemaMap.put(new_file,schema);
////        }
//
//        //generateSchema();
//        while(it.hasNext())
//        {
//            col_name = it.next().toString();
//            if(Data_Storage.join == 1) {
//                if (Data_Storage.project_array.contains(col_name)) {
//                    Column col = new Column(new Table(aliastableName), col_name);
//                    schema.add(col);
//                    to_send.add(iter_string.next().toString());
//                } else {
//                    iter_string.next();
//                }
//            }
//            else
//            {
//                Column col = new Column(new Table(aliastableName), col_name);
//                schema.add(col);
//                to_send.add(iter_string.next().toString());
//            }
//        }
        return str_split;
        //new Tuple(to_send,schema);
    }

    @Override
    public void reset() {
        String file;
        if(filetype)
            file = "data/"+new_file+".dat";
        else
            file = "indexes/"+new_file+".txt";
        try {
            read = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void print()
    {
        System.out.println("FileIterator"+new_file);
    }
    @Override
    public Iterator_Interface getChild(){
        return null;
    }

    @Override
    public void setChild(Iterator_Interface iter) {

    }

    @Override
    public String getFileName() {
        return this.new_file;
    }

    public void generateSchema(){
        //Perform projection push down
        String tablename;
        if(new_file.indexOf("_")!=-1) {
            tablename = new StringTokenizer(new_file, "_").nextToken();
        }
        else
        {
            tablename = new_file;
        }
        LinkedHashMap<String, String> colSchema= Data_Storage.tables.get(tablename);
        int i=0;
        int j=0;
        for(Map.Entry<String, String> map : colSchema.entrySet()){
            String colName = map.getKey();
            if(Data_Storage.projectionCols.contains(colName)){
                String dataType = map.getValue();
                Schema s = new Schema(this.aliastableName,colName,dataType,i);
                this.outSchema.put(colName, s);
                this.inSchema.put(colName,new Schema(this.aliastableName,colName,dataType,j));
                i++;
            }
            j++;
        }
    }

    @Override
    public LinkedHashMap<String, Schema> getSchema(){
        return this.outSchema;
    }
}
