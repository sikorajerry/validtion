/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package org.estat.sdmxsource.engine.decorators;

import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.OutputStream;
import java.util.List;

/**
 * Decorator Class to ReaderEngine
 * <p>Decorators provide a flexible alternative to sub-classing for extending functionality.
 * Decorating an object changes its behavior but not its interface.</p>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1205">SDMXCONV-1205</a>
 */
public abstract class ReaderDecorator implements DataReaderEngine {

	public boolean isFatalErrorHappened() {
		return fatalErrorHappened;
	}

	public void setFatalErrorHappened(boolean fatalErrorHappened) {
		this.fatalErrorHappened = fatalErrorHappened;
	}

	private boolean fatalErrorHappened;

	protected final DataReaderEngine dataReaderEngine;

	protected ReaderDecorator(DataReaderEngine dataReaderEngine) {
		this.dataReaderEngine = dataReaderEngine;
	}

	@Override
	public boolean moveNextObservation() {
		return this.dataReaderEngine.moveNextObservation();
	}

	@Override
	public HeaderBean getHeader() {
		return this.dataReaderEngine.getHeader();
	}

	@Override
	public DataReaderEngine createCopy() {
		return this.dataReaderEngine.createCopy();
	}

	@Override
	public ProvisionAgreementBean getProvisionAgreement() {
		return this.dataReaderEngine.getProvisionAgreement();
	}

	@Override
	public DataflowBean getDataFlow() {
		return this.dataReaderEngine.getDataFlow();
	}

	@Override
	public DataStructureBean getDataStructure() {
		return this.dataReaderEngine.getDataStructure();
	}

	@Override
	public int getDatasetPosition() {
		return this.dataReaderEngine.getDatasetPosition();
	}

	@Override
	public int getKeyablePosition() {
		return this.dataReaderEngine.getKeyablePosition();
	}

	@Override
	public int getObsPosition() {
		return this.dataReaderEngine.getObsPosition();
	}

	@Override
	public DatasetHeaderBean getCurrentDatasetHeaderBean() {
		return this.dataReaderEngine.getCurrentDatasetHeaderBean();
	}

	@Override
	public List<KeyValue> getDatasetAttributes() {
		return this.dataReaderEngine.getDatasetAttributes();
	}

	@Override
	public Keyable getCurrentKey() {
		return this.dataReaderEngine.getCurrentKey();
	}

	@Override
	public Observation getCurrentObservation() {
		return this.dataReaderEngine.getCurrentObservation();
	}

	@Override
	public boolean moveNextKeyable() {
		return this.dataReaderEngine.moveNextKeyable();
	}

	@Override
	public boolean moveNextDataset() {
		return this.dataReaderEngine.moveNextDataset();
	}

	@Override
	public void reset() {
		this.dataReaderEngine.reset();
	}

	@Override
	public void copyToOutputStream(OutputStream outputStream) {
		this.dataReaderEngine.copyToOutputStream(outputStream);
	}

	@Override
	public void close() {
		this.dataReaderEngine.close();
	}

	public DataReaderEngine getDataReaderEngine() {
		return dataReaderEngine;
	}
}
