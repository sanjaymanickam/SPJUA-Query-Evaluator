package edu.buffalo.www.cse4562;

import java.io.*;
import java.util.*;

public class Preprocess {
    static HashMap<String, ArrayList<Integer>> partKeyIndex = new HashMap<>();
    public static boolean preprocessData(){
        Data_Storage.indexColumns.remove("L_PARTKEY");
        Iterator tableIter = Data_Storage.tables.keySet().iterator();
        while (tableIter.hasNext()){
            String tableName = tableIter.next().toString();
//            System.err.println("Processing start - Table - "+tableName);
            HashMap<String, HashSet<Integer>> stats = getStats(tableName);
//            System.err.println("Processing done - Table - "+tableName);
        }
        return true;
    }
    public static HashMap<String, HashSet<Integer>> getStats(String file){
        HashMap<String,HashSet<Integer>> index = new HashMap<>();
        HashSet<String> files = new HashSet<>();
        files.add("LINEITEM");
        files.add("ORDERS");
        boolean toIndex = false;
        BufferedWriter bw;
        BufferedReader buf;
        if(files.contains(file)){
            toIndex = true;
        }
        try {
            ArrayList<String> names = Data_Storage.fKeyNames.get(file);
            ArrayList<Integer> indexes = Data_Storage.fKeyPositions.get(file);
            int tupleCount = 0;
            buf = new BufferedReader(new FileReader(new File("data/"+file+".dat")));
            String line = buf.readLine();
            int c = 0;
            HashSet<String> l_supkey = new HashSet<>();
            HashSet<String> l_orderkey = new HashSet<>();
            HashSet<String> l_partkey = new HashSet<>();
            while (line !=null){
                if(toIndex){
                    for(int i=0;i<names.size();i++) {
                        int pos = indexes.get(i);
                        String colName = names.get(i);
                            if (c == 0) {
//                                System.err.println("Indexing col - " + colName);
                            }
                            StringTokenizer stringTokenizer = new StringTokenizer(line, "|");
                            int count = 0;
                            String value = "";
                            while (stringTokenizer.hasMoreElements()) {
                                if (count == pos) {
                                    value = stringTokenizer.nextElement().toString();
                                    break;
                                }
                                count++;
                                stringTokenizer.nextElement();
                            }
                            if (colName.equals("L_PARTKEY")) {
                                if (partKeyIndex.containsKey(value)) {
                                    partKeyIndex.get(value).add(tupleCount);
                                } else {
                                    ArrayList<Integer> test = new ArrayList<>();
                                    test.add(tupleCount);
                                    partKeyIndex.put(value, test);
                                }
                            } else {
                                String fileName = "indexes/" + file + "_" + colName + "_" + value + ".txt";
                                bw = new BufferedWriter(new FileWriter(new File(fileName), true));
                                try {
                                    bw.write(line);
                                    bw.newLine();
                                    bw.flush();
                                    bw.close();
                                    if (c == 0) {
//                                        System.err.println("New file written - " + fileName);
                                    }
                                } catch (IOException e) {
//                                    System.err.println("Exception while writing");
//                                    System.err.println(e);
                                }
                            }
                    }
                }
                c++;
                tupleCount++;
                line = buf.readLine();
            }
//            System.err.println("Read file");
//            System.err.println(tupleCount);
//            System.err.println(partKeyIndex.size());
            buf.close();
            Data_Storage.tableSize.put(file,tupleCount);
        }
        catch (FileNotFoundException e){
//            System.err.println("Exception File not found");
            e.printStackTrace();
        }
        catch (IOException e){
//            System.err.println("IO ");
            e.printStackTrace();
        }
        return index;
    }
}
