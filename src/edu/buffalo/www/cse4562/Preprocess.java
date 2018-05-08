package edu.buffalo.www.cse4562;

import java.io.*;
import java.util.*;

public class Preprocess {
    public static boolean preprocessData(){
        Iterator tableIter = Data_Storage.tables.keySet().iterator();
        while (tableIter.hasNext()){
            String tableName = tableIter.next().toString();
            //Foreign key indexing
            getStats(tableName);
            //HashMap<String, HashSet<Integer>> stats = getStats(tableName, Data_Storage.foreignKey.get(tableName));
            System.err.println("Done");
        }
        return true;
    }
    public static void getStats(String file){
        HashMap<String,HashSet<Integer>> index = new HashMap<>();
        HashSet<String> files = new HashSet<>();
        try {
            int tupleCount = 0;
            int retFlag = 0;
            int mkt = 0;
            int ship = 0;
            BufferedReader buf = new BufferedReader(new FileReader(new File("data/"+file+".dat")));
            String line = buf.readLine();
            while (line !=null){
                ArrayList<String> tuple = new ArrayList<>();
                StringTokenizer stringTokenizer = new StringTokenizer(line,"|");
                    while (stringTokenizer.hasMoreElements()){
                        tuple.add(stringTokenizer.nextElement().toString());
                    }
                if(file.equals("LINEITEM")){
                    if(tuple.get(8).equals("R")){
                        retFlag++;
                    }
                    if(tuple.get(13).equals("DELIVER IN PERSON")){
                        ship++;
                    }
                }
                else if(file.equals("CUSTOMER")){
                        if(tuple.get(6).equals("BUILDING")){
                            mkt++;
                        }
                }
//                for(ArrayList<String> keys : indexMeta){
//                    int pos = Integer.parseInt(keys.get(1));
//                    String colName = keys.get(0);
//                    ArrayList<String> tuple = new ArrayList<>();
//                    StringTokenizer stringTokenizer = new StringTokenizer(line,"|");
//                    while (stringTokenizer.hasMoreElements()){
//                        tuple.add(stringTokenizer.nextElement().toString());
//                    }
//                    String value = tuple.get(pos);
//                    String fileName = "index/"+file+"_"+colName+"_"+value+".txt";
//                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
//                        out.println(line);
//                    }catch (IOException e) {
//                        System.err.println(e);
//                    }
//                }
                tupleCount++;
                line = buf.readLine();
            }
            System.err.println("TableName : "+file+" ------- "+tupleCount);
            System.err.println("Return Flag : "+file+" ------- "+retFlag);
            System.err.println("Mkt : "+file+" ------- "+mkt);
            System.err.println("Shipinst : "+file+" ------- "+ship);
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
