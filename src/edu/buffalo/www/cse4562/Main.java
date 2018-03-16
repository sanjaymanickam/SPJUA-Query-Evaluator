package edu.buffalo.www.cse4562;

public class Main {
    public static void main(String[] args) {
        try {
            Command_Executor.exec(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
