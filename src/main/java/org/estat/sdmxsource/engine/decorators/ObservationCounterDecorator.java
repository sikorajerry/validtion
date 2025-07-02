/**
 * Copyright (c) 2015 European Commission.
 * <p>
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl5
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package org.estat.sdmxsource.engine.decorators;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.io.data.Formats;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxSyntaxException;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataValidationException;
import org.sdmxsource.sdmx.validation.exceptions.TypeOfException;
import org.sdmxsource.util.ObjectUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Decorator Class to count Observations
 * <p>There is a need to count if a series or the whole file has zero Observations and if this is the case we should throw an error.</p>
 * <p>If the Dataset Action = "Delete" then series without observations are permitted.</p>
 * <p>Right now, the decorator reports only errors when a series exists and doesn't have observations.</p>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1205">SDMXCONV-1205</a>
 */
public class ObservationCounterDecorator extends ReaderDecorator implements ErrorReporter, DataValidationErrorHolder {

	/**
	 * The exception handler
	 */
	private ExceptionHandler exceptionHandler = new FirstFailureExceptionHandler();
	/**
	 * Counts Observations for this series
	 */
	private int observationCounter = 0;
	/**
	 * Variable to count how many times the functions {@link #moveNextDataset()} were called.
	 * This is used to locate at the end of the reading if the input had only one empty dataset or only one empty series.
	 */
	private int iterationDatasetNumber;
	/**
	 * Variable to count how many times the functions {@link #moveNextKeyable()} were called.
	 * This is used to locate at the end of the reading if the input had only one empty dataset or only one empty series.
	 */
	private int iterationSeriesNumber;
	/**
	 * Variable to count how many times the functions {@link #moveNextObservation()} were called.
	 * This is used to locate at the end of the reading if the input had only one empty dataset or only one empty series.
	 */
	private int iterationObservationNumber;
	/**
	 * Indicator if the action is delete in the dataset
	 */
	private boolean isActionDelete;
	/**
	 * Count the times closed is called, because of decorators the reader is closed more than once
	 */
	private int numberOfTimesClosed;
	/**
	 * Input Format
	 */
	private Formats format;
	/**
	 * Variable that is set in the properties file of each component.
	 * <p>ErrorIfEmpty now is configured for all formats and readers.</p>
	 *
	 * @see ConfigService#isValidationErrorIfEmpty()
	 */
	private boolean errorIfEmpty;

	private LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();

	private Integer maximumErrorOccurrencesNumber;

	private int order;

	private int errorLimit;

	private DataflowBean defaultDataflowBean;

	private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();

	@Override
	public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
		return errorPositions;
	}

	@Override
	public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		this.errorPositions = errorPositions;
	}

	@Override
	public DataflowBean getDataFlow() {
		return this.defaultDataflowBean;
	}

	public ObservationCounterDecorator(DataReaderEngine dataReaderEngine, InputConfig inputConfig, Formats format, DataflowBean dataflowBean) {
		super(dataReaderEngine);
		this.defaultDataflowBean = dataflowBean;
		setParameters(dataReaderEngine, inputConfig, format);
	}

	private void setParameters(DataReaderEngine dataReaderEngine, InputConfig inputConfig, Formats format) {
		this.errorIfEmpty = inputConfig != null && inputConfig.isErrorIfEmpty();
		this.format = format;
		if (dataReaderEngine instanceof ErrorReporter) {
			this.exceptionHandler = ((ErrorReporter) dataReaderEngine).getExceptionHandler();
		}
		if(dataReaderEngine instanceof DataValidationErrorHolder) {
			this.errorsByEngine = ((DataValidationErrorHolder) dataReaderEngine).getErrorsByEngine();
			this.maximumErrorOccurrencesNumber = ((DataValidationErrorHolder) dataReaderEngine).getMaximumErrorOccurrencesNumber();
			this.order = ((DataValidationErrorHolder) dataReaderEngine).getOrder();
			this.errorLimit = ((DataValidationErrorHolder) dataReaderEngine).getErrorLimit();
			this.errorPositions =  ((DataValidationErrorHolder) dataReaderEngine).getErrorPositions();
		}
	}

	@Override
	public boolean moveNextObservation() {
		++this.iterationObservationNumber;
		boolean observationExists = super.moveNextObservation();
		if (observationExists) this.observationCounter++;
		return observationExists;
	}

	@Override
	public boolean moveNextKeyable() {
		++this.iterationSeriesNumber;
		//Check if we are processing the first keyable, for the first run it will return null
		//exclude group from the check according to SDMXCONV-1233
		//SDMXCONV-1268
		if (this.errorIfEmpty && !this.isActionDelete
				&& ObjectUtil.validObject(getCurrentKey()) && getCurrentKey().isSeries()
					&& this.observationCounter == 0 && format != Formats.EXCEL && format != Formats.FLR
						&& format != Formats.CSV && format != Formats.SDMX_CSV && format != Formats.SDMX_CSV_2_0) {
			List<Map.Entry<String, ErrorPosition>> entryList = new ArrayList<>(errorPositions.entrySet());
			ErrorPosition errorPosition = entryList.get(entryList.size() - 1).getValue();
			//If we are not in the first run and the current number of observations are 0, then the observation for the previous key did not exist
			this.exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.MISSING_OBS_FROM_SERIES, "series", "Invalid Observation.", errorPosition, getCurrentKey()));
		}
		boolean seriesKeyExists = super.moveNextKeyable();
		if (seriesKeyExists) {
			this.observationCounter = 0; //Reset the counter everytime a new key is read
		}
		return seriesKeyExists;
	}

	@Override
	public boolean moveNextDataset() {
		++this.iterationDatasetNumber;
		boolean datasetExists = super.moveNextDataset();
		//We want to find out the action of the dataset
		DatasetHeaderBean datasetHeaderBean = getCurrentDatasetHeaderBean();
		//If the action is delete and the format is not 2.1 then no error will be thrown
		if (ObjectUtil.validObject(datasetHeaderBean)
				&& datasetHeaderBean.getAction() == DATASET_ACTION.DELETE
					&& this.format!=null
						&& (!this.format.isSdmx21() && !this.format.isSdmx30())) {
			this.isActionDelete = true;
		}
		if (datasetExists){
			this.observationCounter = 0; //Reset the counter everytime a dataset is read
		}
		return datasetExists;
	}

	@Override
	public void close() {
		this.numberOfTimesClosed++;
		if(this.getDataReaderEngine()  instanceof DataValidationErrorHolder) {
			this.errorsByEngine = ((DataValidationErrorHolder) this.getDataReaderEngine()).getErrorsByEngine();
			this.errorPositions = ((DataValidationErrorHolder) this.getDataReaderEngine()).getErrorPositions();
		}
		if(dataReaderEngine instanceof RecordReaderCounter) {
			((RecordReaderCounter) dataReaderEngine).checkErrorIfEmpty(errorIfEmpty, numberOfTimesClosed, dataReaderEngine, isFatalErrorHappened());
		}
		//Avoid to Add the errors more than once if the reader is closed multiple times
		if(this.numberOfTimesClosed == 1 && this.exceptionHandler!=null && this.exceptionHandler instanceof FirstFailureExceptionHandler) {
			this.maximumErrorOccurrencesNumber = 1;
			if(ObjectUtil.validMap(this.errorsByEngine)) {
				List<DataValidationError> errorList = new ArrayList<>();
				this.errorsByEngine.forEach((key, value) -> {
					errorList.addAll(value);
				});
				Collections.sort(errorList, Comparator.comparingInt(DataValidationError::getOrder));
				Iterator<DataValidationError> iter = errorList.iterator();
				while(iter.hasNext()){
					DataValidationError error = iter.next();
					//SDMXCONV-1185
					if (ObjectUtil.validObject(error) && error.isErrorDisplayable()) {
						//Throw as format errors the error from the readers
						if(error.getTypeOfException() == TypeOfException.SdmxDataFormatException) {
							exceptionHandler.handleException(new SdmxDataFormatException(error));
						} else if(error.getTypeOfException() == TypeOfException.SdmxSyntaxException) {
							exceptionHandler.handleException(new SdmxSyntaxException(error.getCode(), error.getErrorMessage()));
						} else {
							exceptionHandler.handleException(new SdmxDataValidationException(error));
						}
						iter.remove(); //remove whatever error was added to exceptionHandler
					}
				}
			}
		}
		super.close();
	}

	public int getObservationCounter() {
		if (dataReaderEngine instanceof RecordReaderCounter) {
			return ((RecordReaderCounter)dataReaderEngine).getObsCount();
		}
		return -1;
	}

	public int getIgnoredObservationCounter() {
		if (dataReaderEngine instanceof RecordReaderCounter) {
			return ((RecordReaderCounter)dataReaderEngine).getIngoredObsCount();
		}
		return -1;
	}

	@Override
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> getErrorsByEngine() {
		return this.errorsByEngine;
	}

	@Override
	public void setErrorsByEngine(LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine) {
		this.errorsByEngine = errorsByEngine;
	}

	@Override
	public Integer getMaximumErrorOccurrencesNumber() {
		return this.maximumErrorOccurrencesNumber;
	}

	@Override
	public void setMaximumErrorOccurrencesNumber(Integer maximumErrorOccurrencesNumber) {
		this.maximumErrorOccurrencesNumber = maximumErrorOccurrencesNumber;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	@Override
	public int getErrorLimit() {
		return this.errorLimit;
	}

	@Override
	public void setErrorLimit(int errorLimit) {
		this.errorLimit = errorLimit;
	}
}
