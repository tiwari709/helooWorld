//Java code to perform sync operation between the two folder on hdfs this java code create jar file with name "Assignment21.jar"

package com.edureka;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

public class SyncFile {
	/**
	 *
	 * @param sourceDir source dir
	 * @param targetDir target dir
	 * @param conf      Hadoop conf
	 * @return
	 */
	public static List<Path> matchFiles(Path sourceDir, Path targetDir, Configuration conf) {
		List<Path> paths = new ArrayList<>();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			org.apache.hadoop.fs.RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(sourceDir, false);
			while (filesIter.hasNext()) {
				LocatedFileStatus fileStatus = filesIter.next();
				String fileName = fileStatus.getPath().getName();
				Path targetFilePath = new Path(targetDir.toString() + "/" + fileName);
				Path sourceFilePath = fileStatus.getPath();
				System.out.println("Checking for file= " + fileName);
				if (fileSystem.exists(targetFilePath)) {
					// Check if both files have same checksum
					FileChecksum sourceCheckSum = fileSystem.getFileChecksum(sourceFilePath);
					FileChecksum targetCheckSum = fileSystem.getFileChecksum(targetFilePath);
					if (sourceCheckSum.equals(targetCheckSum)) {
						System.out.println("File: " + fileName + " has same checksum. Hence NOT copying");
					} else {
					System.out.println("File: " + fileName + " has different checksum. Hence copying");
						FileUtil.copy(fileSystem, sourceFilePath, fileSystem, targetFilePath, false, conf);
						paths.add(sourceFilePath);
					}
				} else {
					System.out.println("Not found Target: " + targetFilePath.toString());
					FileUtil.copy(fileSystem, sourceFilePath, fileSystem, targetFilePath, false, conf);
					paths.add(sourceFilePath);
				}
			}
			fileSystem.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return paths;
	}

	public static void main(String[] args) {
		// String sourceFolder = "hdfs:///user/dfml/data1/src";
		String sourceFolder = args[0];
		// String targetFolder = "hdfs:///user/dfml/data1/target";
		String targetFolder = args[1];
		System.setProperty("HADOOP_CONF", "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop");
		String confPath = System.getProperty("HADOOP_CONF");
		Configuration conf = new Configuration();
		Path hdfsCoreSitePath = new Path(confPath + "/core-site.xml");
		Path hdfsHDFSSitePath = new Path(confPath + "/hdfs-site.xml");
		conf.addResource(hdfsCoreSitePath);
		conf.addResource(hdfsHDFSSitePath);
		List<Path> paths = matchFiles(new Path(sourceFolder), new Path(targetFolder), conf);
	}
}