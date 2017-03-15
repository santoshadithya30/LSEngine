package com.rajuteam.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtils {
	
	public static final String FILE_SEPARATOR_PROPERTY = "file.separator";
	public static final String FILE_SEPARATOR = System.getProperty(FILE_SEPARATOR_PROPERTY);
	public static final String SUCCESS = "SUCCESS";
	public static final String FAILED = "FAILED";

	
	public static boolean createHDFSDirectories(FileSystem aFileSystem, String aDirectoryName){
		boolean isDirCreated = false;
		System.out.println("Working Directory: "+ aFileSystem.getWorkingDirectory());
		
		Path itsWorkingDirectory = new Path(/*aFileSystem.getWorkingDirectory()+FILE_SEPARATOR+*/aDirectoryName);
		
		try {
			if(!aFileSystem.exists(itsWorkingDirectory)){
				isDirCreated = aFileSystem.mkdirs(itsWorkingDirectory);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return isDirCreated;
	}
	
	public static boolean deleteHDFSFileOrDirectory(FileSystem aFileSystem, String aFileName){
		boolean isFileDeleted= false;
		Path itsFilePath = new Path(aFileName);
		try {
			if(aFileSystem.exists(itsFilePath)){
				isFileDeleted = aFileSystem.delete(itsFilePath, true);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return isFileDeleted;
	}
	
	public static String getFileName(final String fileName) {
		String inputFileName = fileName;
		String separator = System.getProperty(FILE_SEPARATOR_PROPERTY);
		int idx = StringUtils.lastIndexOf(fileName, separator);
		if (idx != -1) {
			inputFileName = StringUtils.substring(fileName, idx);
		}
		return inputFileName;
	}
	
	public static boolean copyFromLocal(Configuration conf, FileSystem hdfs,
			String[] args) {
		boolean isCopyFlag = false;
		
		// Step-3 about input file
		String input = args[0];
		final String inputFileName = getFileName(input);

		// Step-4 create MetaData
		final String outputFileName = args[1]
				+ System.getProperty(FILE_SEPARATOR_PROPERTY) + inputFileName;
		System.out.println("Output file name: " + outputFileName);

		Path dstFilePath = new Path(outputFileName);

		InputStream is;
		try {
			is = new FileInputStream(input);
			// OutputStream os = hdfs.create(dstFilePath);
			FSDataOutputStream os = hdfs.create(dstFilePath);
			isCopyFlag = copyBytes(is, os, conf);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return isCopyFlag;
	}
	
	public static boolean copyToLocal(Configuration conf, FileSystem hdfs,
			String[] args) {
		boolean isCopyFlag = false;

		// Step-3 about input file
		String input = args[1];
		//final String inputFileName = getFileName(input);

		// Step-4 create MetaData
		final String outputFileName = args[2];
		System.out.println("Output file will be: " + outputFileName);

		Path dstFilePath = new Path(input);

		OutputStream os;
		try {
			os = new FileOutputStream(outputFileName);
			FSDataInputStream fsIs = hdfs.open(dstFilePath);
			isCopyFlag = copyBytes(fsIs, os, conf);

		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return isCopyFlag;
	}  
	
	public static boolean copyBytes(InputStream is, OutputStream os, Configuration conf)
	{
		boolean isCopySuccess = false;
		try {
			IOUtils.copyBytes(is, os, conf, Boolean.TRUE);
			isCopySuccess = true;
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return isCopySuccess;
	}

}
