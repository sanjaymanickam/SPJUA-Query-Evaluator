package edu.buffalo.www.cse4562;

import java.io.*;
import java.util.*;

public class Preprocess {
    public static boolean preprocessData(){
        Iterator tableIter = Data_Storage.tables.keySet().iterator();
        while (tableIter.hasNext()){
            String tableName = tableIter.next().toString();
            //Foreign key indexing
            System.out.println("Done");
        }
        return true;
    }
    public static HashMap<String, HashSet<Integer>> getStats(String file, ArrayList<ArrayList<String>> indexMeta){
        HashMap<String,HashSet<Integer>> index = new HashMap<>();
        HashSet<String> files = new HashSet<>();
        try {
            int tupleCount = 0;
            BufferedReader buf = new BufferedReader(new FileReader(new File("data/"+file+".dat")));
            String line = buf.readLine();
            while (line !=null){
                if(file.equals("LINEITEM") || file.equals("ORDERS")) {
                    for (ArrayList<String> keys : indexMeta) {
                        int pos = Integer.parseInt(keys.get(1));
                        String colName = keys.get(0);
                        ArrayList<String> tuple = new ArrayList<>();
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
                        String fileName = "indexes/" + file + "_" + colName + "_" + value + ".txt";
                        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
                            out.println(line);
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    }
                }
                tupleCount++;
                line = buf.readLine();
            }
            buf.close();
            Data_Storage.tableSize.put(file,tupleCount);
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return index;
    }
}
