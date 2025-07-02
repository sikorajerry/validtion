package com.intrasoft.sdmx.converter;

/*
 Class that stores metrics of conversion
 */
public class ConverterMetrics {

	long startTime;

	long endTime;

	int observationsIgnored;

	int observationsProcessed;

	long numberOfRows;

	/**
	 * Constructor to set the time metrics
	 * @param startTime
	 * @param endTime
	 */
	public ConverterMetrics(long startTime, long endTime) {
		this.startTime = startTime;
		this.endTime = endTime;
	}

	/**
	 * Constructor to set the observation metrics
	 * @param observationsIgnored
	 * @param observationsProcessed
	 */
	public ConverterMetrics(int observationsIgnored, int observationsProcessed) {
		this.observationsIgnored = observationsIgnored;
		this.observationsProcessed = observationsProcessed;
	}

	/**
	 * @param startTime
	 * @param endTime
	 * @param observationsIgnored
	 * @param observationsProcessed
	 */
	public ConverterMetrics(long startTime, long endTime, int observationsIgnored, int observationsProcessed) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.observationsIgnored = observationsIgnored;
		this.observationsProcessed = observationsProcessed;
	}

	/**
	 * Full Constructor
	 * @param startTime
	 * @param endTime
	 * @param observationsIgnored
	 * @param observationsProcessed
	 * @param numberOfRows
	 */
	public ConverterMetrics(long startTime, long endTime, int observationsIgnored, int observationsProcessed, long numberOfRows) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.observationsIgnored = observationsIgnored;
		this.observationsProcessed = observationsProcessed;
		this.numberOfRows = numberOfRows;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public int getObservationsIgnored() {
		return observationsIgnored;
	}

	public void setObservationsIgnored(int observationsIgnored) {
		this.observationsIgnored = observationsIgnored;
	}

	public int getObservationsProcessed() {
		return observationsProcessed;
	}

	public void setObservationsProcessed(int observationsProcessed) {
		this.observationsProcessed = observationsProcessed;
	}

	public long getNumberOfRows() {
		return numberOfRows;
	}

	public void setNumberOfRows(long numberOfRows) {
		this.numberOfRows = numberOfRows;
	}
}
