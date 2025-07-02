package com.intrasoft.sdmx.converter.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestDateUtil {

	private GroupKeysCache classUnderTest;
	
	@Test
    public void testQuarterly()
    {
        String reportingPeriod = "2010-Q2";
//        String reportingYearStartDate = "--07-01";
        
    	Pattern codeFrequencyPattern = Pattern.compile("[a-zA-Z]");
    	Matcher codeFrequencyIdMatcher = codeFrequencyPattern.matcher(reportingPeriod);
    	if (codeFrequencyIdMatcher.find()) {
    		int firstOccurence = codeFrequencyIdMatcher.start();
    		System.out.println("second works too: " + reportingPeriod.substring(firstOccurence, firstOccurence+1));
    	} else {
    		System.out.println("second doesn't work");
    	}

//        DateTime startDate = new DateTime(2010,10,01,0,0);
//        Reporting reportingTimePeriod = new ReportingTimePeriod();
//        var timePeriod = reportingTimePeriod.ToGregorianPeriod(reportingPeriod,reportingYearStartDate);
//        Assert.That(timePeriod.PeriodStart,Is.EqualTo(startDate));
//        Assert.That(timePeriod.Frequency,Is.EqualTo("Q"));
//
//        var period = reportingTimePeriod.ToReportingPeriod(new SdmxDateCore(timePeriod.PeriodStart, TimeFormatEnumType.QuarterOfYear), reportingYearStartDate);
//        Assert.That(period,Is.EqualTo(reportingPeriod));
    }
	
}