package com.intrasoft.sdmx.converter.integration.tests;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;

public class IntegrationTestsUtils {
	public static final String GENERATED_NAME = "generated_";
	public static final String TARGET_NAME = "./target/";
	public static final String TEST_FILES_NAME = "./testfiles/";

	public static File prettyPrint(File outputFile) throws SAXException, IOException,
			ParserConfigurationException, XPathExpressionException, TransformerException {
		Document document = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder()
				.parse(outputFile);
		document.normalize();

		XPath xPath = XPathFactory.newInstance().newXPath();
		NodeList nodeList = (NodeList) xPath.evaluate("//text()[normalize-space()='']",
				document,
				XPathConstants.NODESET);

		for (int i = 0; i < nodeList.getLength(); ++i) {
			Node node = nodeList.item(i);
			node.getParentNode().removeChild(node);
		}

		/** Remove all comments **/
		NodeList nodeList2 = (NodeList) xPath.evaluate("//comment()", document, XPathConstants.NODESET);
		for (int i = 0; i < nodeList2.getLength(); ++i) {
			Node node = nodeList2.item(i);
			node.getParentNode().removeChild(node);
		}
		// Setup pretty print options
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");

		// Return pretty print xml string
		transformer.transform(new DOMSource(document), new StreamResult(outputFile));
		return outputFile;
	}
}
