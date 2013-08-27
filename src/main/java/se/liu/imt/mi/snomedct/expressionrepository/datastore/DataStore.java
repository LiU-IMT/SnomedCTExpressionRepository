package se.liu.imt.mi.snomedct.expressionrepository.datastore;

import se.liu.imt.mi.snomedct.expressionrepository.api.RelativeAlreadySetException;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyExistsException;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;

import java.util.Date;
import java.util.Set;

import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * The interface to the data store.
 * 
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public interface DataStore {

	/**
	 * Store an expression in the data store.
	 * 
	 * @param expression
	 *            The expression to store.
	 * @param time
	 *            The time the expression was created. A <code>null</code> value
	 *            is handled as the current time.
	 * @return The stored expression's id.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws ExpressionAlreadyExistsException
	 *             Thrown if the expression already exists in the data store.
	 */
	ExpressionId storeExpression(String expression, Date time)
			throws DataStoreException, ExpressionAlreadyExistsException;

	/**
	 * Store that an expression is equivalent to an existing expression in the
	 * data store. This operation also set the parents and children to the
	 * expression.
	 * 
	 * @param id
	 *            The expression's id to store the equivalence for.
	 * @param equivalentExpressionId
	 *            The expression's id to the equivalent expression.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             Thrown if at least one of the expression ids don't exist in
	 *             the data store.
	 * @throws RelativeAlreadySetException
	 *             Thrown if the equivalence already is set or any parent or
	 *             children already has been added to the expression in the data
	 *             store.
	 */
	void storeExpressionEquivalence(ExpressionId id,
			ExpressionId equivalentExpressionId) throws DataStoreException,
			NonExistingIdException, RelativeAlreadySetException;

	/**
	 * Store an expression's parents and children in the data store.
	 * 
	 * @param id
	 *            The expression's id to store the parents and children for.
	 * @param parents
	 *            The expression's parents.
	 * @param children
	 *            The expression's children.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             Thrown if at least one of the expression ids don't exist in
	 *             the data store.
	 * @throws RelativeAlreadySetException
	 *             Thrown if the equivalence already is set or any parent or
	 *             children already has been added to the expression in the data
	 *             store.
	 */
	void storeExpressionParentsAndChildren(ExpressionId id,
			Set<ExpressionId> parents, Set<ExpressionId> children)
			throws DataStoreException, NonExistingIdException,
			RelativeAlreadySetException;

	/**
	 * Get an expression's id from the data store.
	 * 
	 * @param expression
	 *            The expression to receive the id for.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @return The expression's id. If the expression do not exist in the data
	 *         store then <code>null</code> is returned.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	ExpressionId getExpressionId(String expression, Date time)
			throws DataStoreException;

	/**
	 * Get all descendants to a expression at a specific time.
	 * 
	 * @param id
	 *            The expression's id.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @return The descendants' id.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             The expression id do not exists in the data store.
	 */
	Set<ExpressionId> getDescendants(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException;

	/**
	 * Get all children to a expression at a specific time.
	 * 
	 * @param id
	 *            The expression's id.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @return The children's id.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             The expression id do not exists in the data store.
	 */
	Set<ExpressionId> getChildren(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException;

	/**
	 * Get all ancestors to a expression at a specific time.
	 * 
	 * @param id
	 *            The expression's id.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @return The ancestors' id.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             The expression id do not exists in the data store.
	 */
	Set<ExpressionId> getAncestors(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException;

	/**
	 * Get all parents to a expression at a specific time.
	 * 
	 * @param id
	 *            The expression's id.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @return The parents' id.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             The expression id do not exists in the data store.
	 */
	Set<ExpressionId> getParents(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException;

	/**
	 * Get all expressions in the data store.
	 * 
	 * @return All expressions in the data store.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	Set<Expression> getAllExpressions() throws DataStoreException;

}
