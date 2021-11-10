package co.edureka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * To solve this problem write a Java utility which does the following : 1- List
 * the filename : Size for each files recursively in the folder. 2- Finds all
 * HDFS files within input file, which do not contain any data inside them ie
 * size=0. List them in output. 3- Delete all files obtained in step2. List
 * those folders and delete them one by one. Expected output: All empty files
 * within HDFS input folder(recursively) are deleted.
 */

public class DeleteEmptyFile {
	/**
	 * Print all file name and size recursively
	 * 
	 * @param inputDir input folder name
	 * @param conf     hadoop Conf
	 */

	public static void printAllFilesAndSize(Path inputDir, Configuration conf) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			org.apache.hadoop.fs.RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(inputDir, true);
			while (filesIter.hasNext()) {
				LocatedFileStatus fileStatus = filesIter.next();
				long len3 = fileStatus.getLen();
				System.out.println("Name = " + fileStatus.getPath().toString() + " : len= " + len3);
			}
			fileSystem.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * List all the empty files within the empty folder
	 * 
	 * @param inputDir input dir
	 * @param conf     Hadoop conf
	 * @return List of Paths of empty files
	 */
	public static List<Path> listFilesZeroSize(Path inputDir, Configuration conf) {
		List<Path> zeroSizeFiles = new ArrayList<>();
		System.out.println("List of empty files ================");
		int emptyFileCounter = 0;
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			org.apache.hadoop.fs.RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(inputDir, true);
			while (filesIter.hasNext()) {
				LocatedFileStatus fileStatus = filesIter.next();
				if (fileStatus.getLen() == 0) {
					System.out.println("Name = " + fileStatus.getPath().toString() + " : len= " + fileStatus.getLen());
					zeroSizeFiles.add(fileStatus.getPath());
					emptyFileCounter++;
				}
			}
			System.out.println("Number of empty files = " + emptyFileCounter);
			fileSystem.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return zeroSizeFiles;
	}

	/**
	 * Delete the list of paths which are input
	 * 
	 * @param paths Input file paths
	 * @param conf  hadoop conf
	 */
	public static void deleteEmptyFile(List<Path> paths, Configuration conf) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			for (Path path : paths) {
				fileSystem.deleteOnExit(path);
				System.out.println("Deleting file : " + path.toString());
			}
			fileSystem.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		System.out.println("Program to delete all empty files ");
		String confPath = System.getProperty("HADOOP_CONF", "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop");
// String inputFile = "hdfs:///user/dfml/data1";
		String inputFile = args[0];
		Configuration conf = new Configuration();
		Path hdfsCoreSitePath = new Path(confPath + "/core-site.xml");
		Path hdfsHDFSSitePath = new Path(confPath + "/hdfs-site.xml");
		conf.addResource(hdfsCoreSitePath);
		conf.addResource(hdfsHDFSSitePath);
// print file name and sizes
		printAllFilesAndSize(new Path(inputFile), conf);
// print empty file names
		List<Path> emptyFiles = listFilesZeroSize(new Path(inputFile), conf);
// delete all empty files
		deleteEmptyFile(emptyFiles, conf);
	}
}
