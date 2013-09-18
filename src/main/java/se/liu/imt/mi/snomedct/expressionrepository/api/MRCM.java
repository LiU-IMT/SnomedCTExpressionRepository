/**
 * Interface for the Machine Readable Concept Model
 */
package se.liu.imt.mi.snomedct.expressionrepository.api;

import java.io.IOException;

import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 */
public interface MRCM {

	/**
	 * Reads an MRCM input file and stores a representation of that file in
	 * memory
	 * 
	 * @param fileName
	 *            the name of the MRCM input file to be read
	 * @throws IOException
	 */
	void loadMRCM(String fileName) throws IOException;

	/**
	 * Checks whether a subject-relationship-object triple is allowed according
	 * to the current MRCM
	 * 
	 * @param subject
	 *            the subject
	 * @param relationship
	 *            the relationship type
	 * @param object
	 *            the object
	 * @return true iff the subject-relationship-object triple is allowed
	 *         according to the current MRCM
	 * @throws ConceptModelException
	 * @throws DataStoreException
	 */
	boolean validate(ExpressionId subject, ExpressionId relationship,
			ExpressionId object) throws ConceptModelException,
			DataStoreException;

}
