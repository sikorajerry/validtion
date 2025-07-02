package com.intrasoft.sdmx.converter.services;

import java.io.OutputStream;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import javanet.staxutils.IndentingXMLStreamWriter;
import org.sdmxsource.sdmx.dataparser.factory.XMLWriterFactory;

public class ConverterXMLWriterFactoryImpl implements XMLWriterFactory{

	@Override
	public XMLStreamWriter createXMLStreamWriter(OutputStream writerOut) throws XMLStreamException {
		XMLOutputFactory xmlOutputfactory = XMLOutputFactory.newInstance();
        return new IndentingXMLStreamWriter(xmlOutputfactory.createXMLStreamWriter(writerOut, "UTF-8"));
	}
}
