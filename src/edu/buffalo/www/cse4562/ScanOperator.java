package edu.buffalo.www.cse4562;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator implements RelationalOperator {

	BufferedReader input;
	File f;
	
	public  ScanOperator(File f) {
		// TODO Auto-generated constructor stub
		this.f = f;
		reset();
	}
	@Override
	public String[] readTuple() {
		// TODO Auto-generated method stub
		//System.out.println("Reaading actual file from here - Scan operator");
		if(input == null) {return null;}
		String line = null;
		try {
			line = input.readLine();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		if(line == null) {return null;}
		String cols[] = line.split("\\|");
		//System.out.println("Returning cols");
		return cols;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			input = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			input = null;
		}
		
	}

}
