import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

public class Parser {

  public static void main(String[] args) {

	Parser obj = new Parser();
	obj.run(args[0]);

  }

  public void run(String strFolder) {

	File folder = new File(strFolder); 
	File[] fileList = folder.listFiles();

	for (File file : fileList) {
		String csvFile = file.getName();
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

		try {
			File cleanFile = new File(strFolder + "/clean/" + csvFile);

	        	FileWriter writer = new FileWriter(strFolder + "/clean/" + csvFile);

			br = new BufferedReader(new FileReader(strFolder + "/" + csvFile));

			System.out.println("Starting file: " + strFolder + "/clean/" + csvFile);

			while ((line = br.readLine()) != null) {
				String[] data = line.split(cvsSplitBy);
				String newLine = data[6] + "," + data[11] + "," + data[17] + "," + data[26] + "," + data[37] + "," + "\n";
				writer.append(newLine);
			}

			br.close();

			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Done file: " + strFolder + "/clean/" + csvFile);
  	}

  }

}
