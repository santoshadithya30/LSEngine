package com.rajuteam.util;

import java.io.File;

import com.rajuteam.LSEnginePropertiesReader;

public class FileUtils {

	public static boolean createDirectories(String path) {
		boolean isDirCreated = false;
		File aFile = new File(path);
		if (!aFile.exists()) {
			isDirCreated = aFile.mkdirs();
		}
		return isDirCreated;
	}

	public static boolean moveFile(File sourceFile, File destinationPath) {
		return sourceFile.renameTo(new File(destinationPath, sourceFile
				.getName()));
	}

	public static boolean moveFiles(File aSourceDir, File aDestinationDir) {
		boolean isFilesMoved = false;
		if (aSourceDir.isDirectory()) {
			File[] aListFiles = aSourceDir.listFiles();
			int aSourceFilesCnt = aListFiles.length;
			int aMoveFilesCnt = 0;
			for (File aFile : aListFiles) {
				boolean isFileMoved = moveFile(aFile, aDestinationDir);
				if (isFileMoved) {
					aMoveFilesCnt++;
					System.out.println("Success fully moved: " + aFile);

				} else {
					System.out.println("File moved unsuccess full: " + aFile);
				}
			}

			if (aSourceFilesCnt == aMoveFilesCnt) {
				isFilesMoved = true;
				System.out.println("All files moved successfully.");
			} else if (aMoveFilesCnt > 0 && aMoveFilesCnt < aSourceFilesCnt) {
				System.out.println("Files moved partially.");
			} else {
				System.out.println("Files are not moved.");
			}
		}
		return isFilesMoved;
	}

	public static boolean deleteFile(File aFile) {
		return aFile.delete();
	}

	public boolean deleteDirectory(File aDirectory) {

		boolean isDirDeleted = false;
		if (aDirectory.isDirectory()) {
			if (aDirectory.listFiles().length == 0) {
				if (!(aDirectory.getAbsolutePath())
						.equalsIgnoreCase(LSEnginePropertiesReader
								.getInstance().getProperty(
										LSEngineProperties.BASE_LOCATION)))
					isDirDeleted = aDirectory.delete();
				System.out.println("Deleted: " + aDirectory.getAbsolutePath());
				deleteDirectory(aDirectory.getParentFile());
			}
		}
		return isDirDeleted;
	}
}
