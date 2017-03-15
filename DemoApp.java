package com.rajuteam;


public class DemoApp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LSEnginePropertiesReader lseProps = LSEnginePropertiesReader.getInstance();
		lseProps.loadProperties("C:/Users/USER/Desktop/raja.txt");
		System.out.println(lseProps.getAllProperties().toString());
		LSEnginePropertiesReader lseProps2 = LSEnginePropertiesReader.getInstance();
		lseProps2.loadProperties("C:/Users/USER/Desktop/test.properties");
		System.out.println(lseProps2.getAllProperties().toString());
	}

}
