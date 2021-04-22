package project.ADB2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.htyleo.extsort.ExternalSort;
import com.htyleo.extsort.ExternalSortConfig;

public class ExternalMergeSort {
	private static ArrayList<String> tempFilesA; 
	private static ArrayList<String> tempFilesB;
	static JSONArray tempFiles1;
	static JSONArray tempFiles2;
	static String key = "year";
	//Chunksize represents the no. of rows in each sub document 
	private static int chunksize = 16;
	//Output file
	private static String outputFile;

	public static void main(String[] args) {
		//Store input file url
		String inputFile = args[0];
	
		//Check if file exists
		if (!checkFile(inputFile)) {
			System.out.println("Invalid input file");
			System.exit(1);
		}
		//Check if output file exists
		if (args.length < 2) {
			System.out.println("No ouput file specified");
			System.exit(1);
		}
		//Read the output file url
		outputFile = args[1];
		//Initialize temp files A and B with empty array lists
		tempFilesA = new ArrayList<String>();
		tempFilesB = new ArrayList<String>();
		tempFiles1 = new JSONArray();
		tempFiles2 = new JSONArray();
		//If the operation of break_input method is successful
		//then merge the chunks in next pass
		if (!breakInput(inputFile)) {
			int pass = 1, chunk = 0;
			String oddFile = null;

			/*
			 * This loop is the two-file merge. The names of the files are
			 * stored in two arrays so that the names don't have to be recalled
			 * using a File directory scanner
			 */
			while (true) {
				boolean even = false;
				chunk = 0;
				if (pass % 2 == 1) {
					if (tempFilesA.size() == 1)
						break;
					if (tempFilesA.size() % 2 == 0)
						even = true;
					int a = 0, b = 1;
					for (int i = 0; i < tempFilesA.size() / 2; i++) {
						mergeTempFiles(tempFilesA.get(a), tempFilesA.get(b), pass, chunk, false);
						chunk++;
						a = a + 2;
						b = b + 2;
					}
					if (!even && oddFile == null) {
						oddFile = tempFilesA.get(tempFilesA.size() - 1);
					} else if (!even && oddFile != null) {
						mergeTempFiles(oddFile, tempFilesA.get(tempFilesA.size() - 1), pass, chunk, false);
						oddFile = null;
					}
					tempFilesA.clear();
				} else {
					if (tempFilesB.size() == 1)
						break;
					if (tempFilesB.size() % 2 == 0)
						even = true;
					int a = 0, b = 1;
					for (int i = 0; i < tempFilesB.size() / 2; i++) {
						mergeTempFiles(tempFilesB.get(a), tempFilesB.get(b), pass, chunk, false);
						chunk++;
						a = a + 2;
						b = b + 2;
					}
					if (!even && oddFile == null) {
						oddFile = tempFilesB.get(tempFilesB.size() - 1);
					} else if (!even && oddFile != null) {
						mergeTempFiles(oddFile, tempFilesB.get(tempFilesB.size() - 1), pass, chunk, false);
						oddFile = null;
					}
					tempFilesB.clear();
				}
				pass++;
			}
			// write output to file specified in args[1]
			if (oddFile != null) {
				if (tempFilesA.size() == 1)
					mergeTempFiles(oddFile, tempFilesA.get(tempFilesA.size() - 1), pass - 1, 0, true);
				if (tempFilesB.size() == 1)
					mergeTempFiles(oddFile, tempFilesB.get(tempFilesB.size() - 1), pass - 1, 0, true);
			} else if (tempFilesA.size() == 1) {
				File f = new File(tempFilesA.get(0)), o = new File(outputFile);
				f.renameTo(o);
			} else if (tempFilesB.size() == 1) {
				File f = new File(tempFilesB.get(0)), o = new File(outputFile);
				f.renameTo(o);
			}
		}
	}

	private static boolean checkFile(String inputFile) {
		//Store input file in f
		File f = new File(inputFile);
		//Return respective boolean if file exists
		return f.exists();
	}

	//Merge files and write to chunk
	private static void mergeTempFiles(String tmp1, String tmp2, int pass, int chunk, boolean out) {
		ArrayList<String> buff = new ArrayList<String>();
		FileReader xms1, xms2;
		String word1 = "", word2 = "";
		org.json.simple.JSONArray js1;
		org.json.simple.JSONArray js2;
		org.json.JSONArray js3 = new JSONArray();
		//JSONObject j1 = new JSONObject();
		//JSONObject j2 = new JSONObject();
		org.json.simple.JSONObject jo1 = new org.json.simple.JSONObject();
		org.json.simple.JSONObject jo2 = new org.json.simple.JSONObject();
		String s1, s2;
		try {
			xms1 = new FileReader(tmp1);
			xms2 = new FileReader(tmp2);
			boolean cont1 = true, cont2 = true;
			BufferedReader br1 = new BufferedReader(xms1), br2 = new BufferedReader(xms2);
			
			try {
				JSONParser parser = new JSONParser();
				js1 = (org.json.simple.JSONArray) parser.parse(br1.readLine());
				js2 = (org.json.simple.JSONArray) parser.parse(br2.readLine());
				int i=0, j=0;
				while (true) {				 
					if (cont1)
						if (i > js1.size()-1 || (jo1 = (org.json.simple.JSONObject) js1.get(i)) == null)
							break;
					if (cont2)
						if (j > js2.size()-1 || (jo2 = (org.json.simple.JSONObject) js2.get(j)) == null)
							break;
					//jo1 = (org.json.JSONObject) js1.get(i);
					s1 = (String) jo1.get(key);
					s2 = (String) jo2.get(key);
					if (s1.compareTo(s2) <= 0) {
						//buff.add(jo1);
						js3.put(jo1);
						cont2 = false;
						cont1 = true;
						i++;
					} else {
						//buff.add(word2);
						js3.put(jo2);
						cont1 = false;
						cont2 = true;
						j++;
					}				
				}
				if (i <= js1.size()-1 && (jo1 = (org.json.simple.JSONObject) js1.get(i)) != null) {
					//buff.add(word1);
					js3.put(jo1);
					i++;
					while (i <= js1.size()-1 && (jo1 = (org.json.simple.JSONObject) js1.get(i)) != null) {
						//buff.add(word1);
						js3.put(jo1);
						i++;
					}
				}
				if (j <= js2.size()-1 && (jo2 = (org.json.simple.JSONObject) js2.get(j)) != null) {
					//buff.add(word2);
					js3.put(jo2);
					j++;
					while (j <= js2.size()-1 && (jo2 = (org.json.simple.JSONObject) js2.get(j)) != null) {
						//buff.add(word2);
						js3.put(jo2);
						j++;
					}
				}
				System.out.println("Merging chunks");
				buff.add(js3.toString());
				writeChunk(buff, pass, chunk, out);
				br1.close();
				br2.close();
			} catch (IOException | ParseException e) {
				System.out.println("There is an error with the input files.");
				System.exit(1);
			}

		} catch (FileNotFoundException e) {
			System.out.println("The requested file was not found.");
			System.exit(1);
		}
	}

	//Write to individual sub chunks
	//words - sorted sub file
	//pass - pass no.
	//chunk - chunk no.
	private static void writeChunk(ArrayList<String> words, int pass, int chunk, boolean out) {
		try {
			//Create a file with the path if out is false
			//Otherwise write sorted records to output file directly
			if (!out) {
				//Create paths for file -- format pass no. with four 0s and chunk with four 0s
				Path file = Paths
						.get("xms.tmp.pass_" + String.format("%04d", pass) + ".chunk_" + String.format("%04d", chunk));

				//Even pass chunks - store in temp files A
				//Odd pass chunks - store in temp files B
				if (pass % 2 == 0) {
					tempFilesA.add(
							"xms.tmp.pass_" + String.format("%04d", pass) + ".chunk_" + String.format("%04d", chunk));
				} else {
					tempFilesB.add(
							"xms.tmp.pass_" + String.format("%04d", pass) + ".chunk_" + String.format("%04d", chunk));
				}
				//Write the file with sorted records
				//String s = words.toString().replaceAll("},*", "},\n");
				//String str[] = words.toString().split("},*");
				//List<String> al = new ArrayList<String>();
				//al = Arrays.asList(str);
				Files.write(file, words, Charset.forName("UTF-8"));
			} 
			else {
				Path file = Paths.get(outputFile);
				//String str[] = words.toString().split("},*");
				//List<String> al = new ArrayList<String>();
				//al = Arrays.asList(str);
				Files.write(file, words, Charset.forName("UTF-8"));
			}
		} catch (IOException e) {
			System.out.println("There is an error with the input files.");
			System.exit(1);
		}
	}

	
	//Sorting two records
	private static ArrayList<String> sortChunk(JSONArray words) {
		// Compares values to lowercase
		for (int i = 0; i < words.length(); i++) {
			for (int j = 0; j <= i; j++) {
				String s1, s2;
				try {
					org.json.JSONObject jo1 = words.getJSONObject(i);
					org.json.JSONObject jo2 = words.getJSONObject(j);
					s1 = (String) jo1.get(key);
					s2 = (String) jo2.get(key);
					//System.out.println(jo1);
					//System.out.println(jo2);
					if (s1.compareTo(s2) <= 0) {
						// swap
						org.json.JSONObject temp = jo2;
						words.put(j, jo1);
						words.put(i, temp);
					}
					
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
		System.out.println("Sorting chunk");
		ArrayList<String> al = new ArrayList<String>();
		al.add(words.toString());
		return al;
	}

	
	private static boolean breakInput(String inputFile) {
		// create file from string/path
		File file = new File(inputFile);
		// load file into scanner
		try {
			Scanner input = new Scanner(file);
			int chunk = 0, pass = 0, stringcount = 0;
			//
			while (input.hasNext()) {
				JSONArray js = new JSONArray();
				JSONObject jo = new JSONObject();
				//Read each input from file and store in temp array list
				//iterate till chunk_size and increment string_count
				int i = 0;
				for (; input.hasNext() && i < chunksize; i++) {
					//temp.add(input.next());
					JSONParser parser = new JSONParser();  
					jo = (JSONObject) parser.parse(input.next()); 
					js.put(jo);
					stringcount++;
				}
				//Checking if the input file is smaller than chunk_size
				//sort it and write_chunk
				if (!input.hasNext() && stringcount < chunksize) {
					ArrayList<String> al = sortChunk(js);
					writeChunk(al, pass, chunk, true);
					input.close();
					return true;
				}
				//Otherwise sort the respective chunk
				//add chunk string to chunks list
				ArrayList<String> al = sortChunk(js);
				writeChunk(al, pass, chunk, false);
				chunk++;
			}
			input.close();
		} catch (FileNotFoundException | ParseException e) {
			System.out.println("The requested file was not found.");
			System.exit(1);
		}
		return false;
	}
}
