/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.api;

import java.util.Collection;
import java.util.Date;

import se.liu.imt.mi.snomedct.expression.tools.ExpressionSyntaxError;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */

public interface ExpressionRepository {

	/**
	 * Returns an expression id object from a string containing a SNOMED CT
	 * post-coordinated expression. If the expression exists in the repository,
	 * the existing id is returned, otherwise a new expression id is generated
	 * and returned.
	 * 
	 * @param expression
	 *            A <code>String</code> containing an expression according to
	 *            the SNOMED CT Compositional Grammar.
	 * @return An expression id object.
	 * @exception ExpressionSyntaxError
	 *                There is a syntax error according to the SNOMED CT
	 *                Compositional Grammer in the <code>String</code>.
	 * @exception NonExistingIdException
	 *                There is some id in the expression that can not be found
	 *                in the repository (including pre-coordinated SNOMED CT
	 *                content).
	 */
	ExpressionId getExpressionID(String expression)
			throws ExpressionSyntaxError, NonExistingIdException;

	/**
	 * Returns an expression as a <code>String</code> according to the SNOMED CT
	 * Compositional Grammar from an <code>ExpressionId</code> object.
	 * 
	 * @param id
	 *            An <code>ExpressionId</code> object
	 * @return A <code>String</code> containing an expression according to the
	 *         SNOMED CT Compositional Grammar. No text will be included in the
	 *         expression.
	 * @exception NonExistingIdException
	 *                An id does not exist in the repository.
	 */
	String getExpression(ExpressionId id) throws NonExistingIdException;

	/**
	 * Returns results of the execution of a query given an
	 * <code>ExpressionId</code>
	 * 
	 * @param id
	 *            An <code>ExpressionId</code> object
	 * @return A <code>Collection</code> of <code>ExpressionId</code>
	 *         representing all descendants
	 * @throws NonExistingIdException
	 *             The id does not exist in the repository.
	 * @throws DataStoreException
	 * @throws Exception
	 */
	Collection<ExpressionId> getSCTQueryResult(String queryExpression)
			throws ExpressionSyntaxError, NonExistingIdException,
			DataStoreException, Exception;

	/**
	 * Returns all descendants of an expression given an
	 * <code>ExpressionId</code>
	 * 
	 * @param expression
	 *            A <code>String</code> containing an expression according to
	 *            the SNOMED CT Query Specification.
	 * @return A <code>Collection</code> of <code>ExpressionId</code>
	 *         representing result as IDs
	 * @exception ExpressionSyntaxError
	 *                There is a syntax error according to the SNOMED CT Query
	 *                Specification and/or SNOMED CT Compositional Grammer in
	 *                the <code>String</code>.
	 * @throws NonExistingIdException
	 *             An id does not exist in the repository.
	 * @throws DataStoreException
	 */
	Collection<ExpressionId> getDecendants(ExpressionId id)
			throws NonExistingIdException, DataStoreException;

	/**
	 * Returns all direct descendants (children) of an expression given an
	 * <code>ExpressionId</code>
	 * 
	 * @param id
	 *            An <code>ExpressionId</code> object
	 * @return A <code>Collection</code> of <code>ExpressionId</code>
	 *         representing all children
	 * @throws NonExistingIdException
	 *             The id does not exist in the repository.
	 * @throws DataStoreException
	 */
	Collection<ExpressionId> getChildren(ExpressionId id)
			throws NonExistingIdException, DataStoreException;

	/**
	 * Returns all ancestors of an expression given an <code>ExpressionId</code>
	 * 
	 * @param id
	 *            An <code>ExpressionId</code> object
	 * @return A <code>Collection</code> of <code>ExpressionId</code>
	 *         representing all ancestors
	 * @throws NonExistingIdException
	 *             The id does not exist in the repository.
	 * @throws DataStoreException
	 */
	Collection<ExpressionId> getAncestors(ExpressionId id)
			throws NonExistingIdException, DataStoreException;

	/**
	 * Returns all direct ancestors (parents) of an expression given an
	 * <code>ExpressionId</code>
	 * 
	 * @param id
	 *            An <code>ExpressionId</code> object
	 * @return A <code>Collection</code> of <code>ExpressionId</code>
	 *         representing all parents
	 * @throws NonExistingIdException
	 *             The id does not exist in the repository.
	 * @throws DataStoreException
	 */
	Collection<ExpressionId> getParents(ExpressionId id)
			throws NonExistingIdException, DataStoreException;

	/**
	 * Checks subsumption between two <code>ExpressionId</code> objects
	 * 
	 * @param id1
	 *            An <code>ExpressionId</code> object
	 * @param id2
	 *            An <code>ExpressionId</code> object
	 * @return true iff id1 is subsumed by id2
	 * @throws DataStoreException
	 */
	boolean isSubsumedNotEquivalent(ExpressionId id1, ExpressionId id2,
			Date time) throws DataStoreException;

}
