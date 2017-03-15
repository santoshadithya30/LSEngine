package com.rajuteam;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rajuteam.util.FileUtils;
import com.rajuteam.util.HDFSUtils;
import com.rajuteam.util.LSEngineProperties;

public class CronJob extends Configured implements Tool {

	private static LSEnginePropertiesReader lsEnginePropertiesReader;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("In MAIN Method");

		// Step-1 validate input arguments
		if (args.length < 1) {
			System.out
					.println("Java Usage "
							+ CronJob.class.getName()
							+ "In valid arguments lenth. Must required LSEnine properties file path.");
			return;
		}

		lsEnginePropertiesReader = LSEnginePropertiesReader.getInstance();
		lsEnginePropertiesReader.loadProperties(args[0]);

		// Step-2 Initialize configuration
		Configuration tConf = new Configuration(Boolean.TRUE);
		tConf.set("fs.defaultFS", "hdfs://localhost:8020");

		// Step-3 Run ToolRunner.run method to set the arguments to config.
		try {
			int i = ToolRunner.run(tConf, new CronJob(), args);
			if (i == 0) {
				System.out.println(HDFSUtils.SUCCESS);
			} else {
				System.out.println(HDFSUtils.FAILED + " STATUS Code: " + i);
			}
		} catch (Exception e) {
			System.out.println(HDFSUtils.FAILED);
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] paramArrayOfString) throws Exception {
		System.out.println("In Run Method");

		final String tBaseLocation = lsEnginePropertiesReader
				.getProperty(LSEngineProperties.BASE_LOCATION);
		final String tFileSourceLocation = tBaseLocation
				+ HDFSUtils.FILE_SEPARATOR
				+ lsEnginePropertiesReader
						.getProperty(LSEngineProperties.LANDING_ZONE);
		// Create directory if does not exist.
		FileUtils.createDirectories(tFileSourceLocation);
		final String tArchiveLocation = tBaseLocation
				+ HDFSUtils.FILE_SEPARATOR
				+ lsEnginePropertiesReader
						.getProperty(LSEngineProperties.ARCHIVE);
		FileUtils.createDirectories(tArchiveLocation);
		final String tFailedLocation = tBaseLocation
				+ HDFSUtils.FILE_SEPARATOR
				+ lsEnginePropertiesReader
						.getProperty(LSEngineProperties.FAILED);

		final String tHDFSBaseLocation = lsEnginePropertiesReader
				.getProperty(LSEngineProperties.HDFS_BASE_LOCATION);
		final String tDestinationPath = tHDFSBaseLocation
				+ HDFSUtils.FILE_SEPARATOR
				+ lsEnginePropertiesReader
						.getProperty(LSEngineProperties.HDFS_LANDING_ZONE);

		// Load the configuration
		Configuration tConf = getConf();

		// Create a instance for File System object.
		FileSystem hdfs = FileSystem.get(tConf);
		// Create directory on HDFS File System if does not exist.
		HDFSUtils.createHDFSDirectories(hdfs, tDestinationPath);

		while (true) {
			File tInboxDir = new File(tFileSourceLocation);
			if(tInboxDir.isDirectory())
			{
				File[] tListFiles = tInboxDir.listFiles();
				for (File tInputFile : tListFiles) {
					String [] args= {tInputFile.getAbsolutePath().toString(), tDestinationPath}; 
					boolean isCopied = HDFSUtils.copyFromLocal(tConf, hdfs, args);
					if(isCopied){
						FileUtils.moveFile(tInputFile, new File(tArchiveLocation));
					} else {
						FileUtils.moveFile(tInputFile, new File(tFailedLocation));
					}
					
				}
			}
			Thread.sleep(1000 * 60 * 5);
		}
	}
}
