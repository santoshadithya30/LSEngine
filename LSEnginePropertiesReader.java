package com.rajuteam;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class LSEnginePropertiesReader {

	private static LSEnginePropertiesReader lsEnginePropertiesReader;
	private Properties lsEngineProperties;

	public static LSEnginePropertiesReader getInstance() {
		if (lsEnginePropertiesReader == null) {
			lsEnginePropertiesReader = new LSEnginePropertiesReader();
		}
		return lsEnginePropertiesReader;

	}

	public void loadProperties(String propertiesFilePath) {
		if (lsEngineProperties == null) {
			lsEngineProperties = new Properties();
		}

		try {
			lsEngineProperties.load(new FileInputStream(propertiesFilePath));

		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public String getProperty(String key) {
		return lsEngineProperties.getProperty(key);
	}

	public int getInt(String key) {
		return Integer.parseInt(lsEngineProperties.getProperty(key));
	}

	public boolean getBoolean(String key) {
		return Boolean.parseBoolean(lsEngineProperties.getProperty(key));
	}

	public double getDouble(String key) {
		return Double.parseDouble(lsEngineProperties.getProperty(key));
	}

	public float getFloat(String key) {
		return Float.parseFloat(lsEngineProperties.getProperty(key));
	}
	
	public Properties getAllProperties()
	{
		return lsEngineProperties;
	}
}
