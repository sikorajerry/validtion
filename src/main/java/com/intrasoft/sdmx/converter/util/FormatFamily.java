package com.intrasoft.sdmx.converter.util;

public enum FormatFamily {
    //SDMXCONV-1141
	SDMX("xml"), EXCEL("xlsx"), CSV("csv"), GESMES("xml"), FLR("flr"), iSDMX_CSV("csv"), UNKNOWN("");
	
	private String fileExtension;
	
	private FormatFamily(String fileExtension) {
		this.fileExtension = fileExtension;
	}
	
	public String getFileExtension() {
		return fileExtension;
	}
}
