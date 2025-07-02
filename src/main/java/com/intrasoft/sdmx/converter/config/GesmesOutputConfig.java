package com.intrasoft.sdmx.converter.config;

import com.intrasoft.sdmx.converter.io.data.TsTechnique;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;

public class GesmesOutputConfig implements OutputConfig {
	
	private TsTechnique gesmeswritingtechnique;

	public TsTechnique getGesmeswritingtechnique() {
		return gesmeswritingtechnique;
	}

	public void setGesmeswritingtechnique(TsTechnique gesmeswritingtechnique) {
		this.gesmeswritingtechnique = gesmeswritingtechnique;
	}
		
}
