/**
 * Interface for the Machine Readable Concept Model
 */
package se.liu.imt.mi.snomedct.expressionrepository.api;

import java.io.IOException;

import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * @author daniel
 *
 */
public interface MRCM {
	
	void loadMRCM(String fileName) throws IOException;
	boolean validate(ExpressionId subject, ExpressionId relationship, ExpressionId object) throws ConceptModelException;

}
