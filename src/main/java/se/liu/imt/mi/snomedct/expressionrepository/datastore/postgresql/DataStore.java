package se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import se.liu.imt.mi.snomedct.expressionrepository.api.RelativeAlreadySetException;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyExistsException;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * An implementation of the <code>DataStore</code> interface for the PostgreSQL
 * database management system.
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class DataStore implements
		se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore {

	/**
	 * The connection to the PostgreSQL database management system, dbms,
	 * containing the expression database.
	 */
	protected Connection con;

	/**
	 * A <code>PreparedStatement</code> that store an a expression without
	 * normal form with the current timestamp in the dbms.
	 */
	private final PreparedStatement storeExpressionPs;

	/**
	 * A <code>PreparedStatement</code> that store an a expression at with a
	 * specified timestamp in the dbms.
	 */
	private final PreparedStatement storeExpressionTimePs;

	/**
	 * A <code>PreparedStatement</code> that set an expression's equivalent id
	 * to the expressions own id in the dbms.
	 */
	private final PreparedStatement setEquivalentIdToIdPs;

	/**
	 * A <code>PreparedStatement</code> that store an expression's equivalent
	 * concept or expression id in the dbms. The stored equivalent id is always
	 * the same for an equivalent group.
	 */
	private final PreparedStatement setEquivalentIdPs;

	/**
	 * A <code>PreparedStatement</code> that create a temporary table for the
	 * parents' id.
	 */
	private final PreparedStatement setParentsTableCreate;

	/**
	 * A <code>PreparedStatement</code> that create a temporary table for the
	 * children's id.
	 */
	private final PreparedStatement setChildrenTableCreate;

	/**
	 * A <code>PreparedStatement</code> that insert the parents id in the
	 * temporary table.
	 */
	private final PreparedStatement setParentsTableInsert;

	/**
	 * A <code>PreparedStatement</code> that insert the children id in the
	 * temporary table.
	 */
	private final PreparedStatement setChildrenTableInsert;

	/**
	 * A <code>PreparedStatement</code> that analyze the temporary table for the
	 * parents' id.
	 */
	private final PreparedStatement setParentsTableAnalyze;

	/**
	 * A <code>PreparedStatement</code> that analyze the temporary table for the
	 * children's id.
	 */
	private final PreparedStatement setChildrenTableAnalyze;

	/**
	 * A <code>PreparedStatement</code> that inserts the parents into the
	 * transitive closure table.
	 */
	private final PreparedStatement setParentsTransitiveclosureParentsInsert;

	/**
	 * A <code>PreparedStatement</code> that inserts the children into the
	 * transitive closure table.
	 */
	private final PreparedStatement setChildrenTransitiveclosureChildrenInsert;

	/**
	 * A <code>PreparedStatement</code> that inserts the ancestors into the
	 * transitive closure table.
	 */
	private final PreparedStatement setParentsTransitiveclosureAncestorsInsert;

	/**
	 * A <code>PreparedStatement</code> that inserts the descendants into the
	 * transitive closure table.
	 */
	private final PreparedStatement setChildrenTransitiveclosureDescendantsInsert;

	/**
	 * A <code>PreparedStatement</code> that store the current direct
	 * relationships as indirect relationships that are out of date due to a
	 * insert of a new expression in the dbms.
	 * 
	 */
	private final PreparedStatement convertDirectToIndirectRelationshipCreatePs;

	/**
	 * A <code>PreparedStatement</code> that retire the current direct
	 * relationships that are out of date due to a insert of a new expression in
	 * the dbms.
	 * 
	 */
	private final PreparedStatement convertDirectToIndirectRelationshipRetirePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's id given
	 * the expression itself from the dbms.
	 */
	private final PreparedStatement getExpressionIdPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's id given
	 * the expression itself at a specific time from the dbms.
	 */
	private final PreparedStatement getExpressionIdTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression given the
	 * expression's id from the dbms.
	 */
	private final PreparedStatement getExpressionPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression given the
	 * expression's id at a specific time from the dbms.
	 */
	private final PreparedStatement getExpressionTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's
	 * descendants at the current time from the dbms.
	 */
	private final PreparedStatement getDescendantsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's
	 * descendants at a specific time from the dbms.
	 */
	private final PreparedStatement getDescendantsTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's children
	 * at the current time from the dbms.
	 */
	private final PreparedStatement getChildrenPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's children
	 * at a specific time from the dbms.
	 */
	private final PreparedStatement getChildrenTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's ancestors
	 * at the current time from the dbms.
	 */
	private final PreparedStatement getAncestorsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's ancestors
	 * at a specific time from the dbms.
	 */
	private final PreparedStatement getAncestorsTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's parents at
	 * the current time from the dbms.
	 */
	private final PreparedStatement getParentsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's parents at
	 * the current time from the dbms.
	 */
	private final PreparedStatement getParentsTimePs;

	/**
	 * A <code>PreparedStatement</code> that retrieve all expressions from the
	 * dbms.
	 */
	private final PreparedStatement getAllExpressionsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve all expressions at a
	 * specific time from the dbms.
	 */
	private final PreparedStatement getAllExpressionsTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * id exists at the current time in the dbms.
	 */
	private final PreparedStatement isExistingIdPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * id exists at a specific time in the dbms.
	 */
	private final PreparedStatement isExistingIdTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept id exists in
	 * the dbms.
	 */
	private final PreparedStatement isExistingConceptIdPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept id exists at
	 * a specific time in the dbms.
	 */
	private final PreparedStatement isExistingConceptIdTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an expression id exists
	 * in the dbms.
	 */
	private final PreparedStatement isExistingExpressionIdPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an expression id exists
	 * at a specific time in the dbms.
	 */
	private final PreparedStatement isExistingExpressionIdTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * subsumes but is not equivalent to another concept or expression at the
	 * current time.
	 */
	private final PreparedStatement isSubsumingNotEquivalentPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * subsumes but is not equivalent to another concept or expression at a
	 * specific time.
	 */
	private final PreparedStatement isSubsumingNotEquivalentTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * is equivalent to another concept or expression at the current time.
	 */
	private final PreparedStatement isEquivalentPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * is equivalent to another concept or expression at a specific time.
	 */
	private final PreparedStatement isEquivalentTimePs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * subsumes another concept or expression at the current time.
	 */
	@SuppressWarnings("unused")
	private final PreparedStatement isSubsumingPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression
	 * subsumes another concept or expression at a specific time.
	 */
	@SuppressWarnings("unused")
	private final PreparedStatement isSubsumingTimePs;

	// ----------

	/**
	 * A <code>PreparedStatement</code> that check if an expression's equivalent
	 * concept or expression id is set in the dbms.
	 */
	private final PreparedStatement iSEquivalentIdSetPs;

	/**
	 * A <code>PreparedStatement</code> that check if an expression has any
	 * relatives set in the dbms.
	 */
	private final PreparedStatement isRelativeSetPs;

	/**
	 * Creates a data store API and set up a connection to the PostgreSQL
	 * database management system containing the expression database.
	 * 
	 * @param url
	 *            The URL for the database connection.
	 * @param userName
	 *            The user name for the database connection.
	 * @param password
	 *            The user password for the database connection.
	 * @throws DataStoreException
	 *             Thrown if there is a problem with the dbms or the connection
	 *             to the dbms.
	 */
	public DataStore(final String url, final String userName,
			final String password) throws DataStoreException {
		super();

		// Set up the dbms connection.
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new DataStoreException(e);
		}
		try {
			con = DriverManager.getConnection(url, userName, password);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}

		// Creates the prepared statements.
		try {
			storeExpressionPs = con
					.prepareStatement("INSERT INTO expressions (expression, starttime) VALUES (?, now());");
			storeExpressionTimePs = con
					.prepareStatement("INSERT INTO expressions (expression, starttime) VALUES (?, ?);");
			setEquivalentIdToIdPs = con
					.prepareStatement("UPDATE expressions SET equivalentid = id WHERE expression = ?;");
			iSEquivalentIdSetPs = con
					.prepareStatement("SELECT id <> equivalentid AS set FROM expressions WHERE id = ?;");
			isRelativeSetPs = con
					.prepareStatement("SELECT source.set OR destination.set "
							+ "FROM (SELECT Count(*) > 0 AS set FROM transitiveclosure "
							+ "WHERE sourceid IN (SELECT equivalentid FROM conexp WHERE id = ?)) AS source, "
							+ "(SELECT Count(*) > 0 AS set FROM transitiveclosure "
							+ "WHERE destinationid IN (SELECT equivalentid FROM conexp WHERE id = ?)) AS destination;");
			setEquivalentIdPs = con
					.prepareStatement("UPDATE expressions SET equivalentid = "
							+ "(SELECT Max(equivalentid) FROM conexp WHERE endtime IS NULL AND id = ?) "
							+ "WHERE id = ?;");
			setParentsTableCreate = con
					.prepareStatement("CREATE TEMPORARY TABLE parents "
							+ "(id bigint NOT NULL, CONSTRAINT \"PK_parents\" PRIMARY KEY (id)) "
							+ "ON COMMIT DROP;");
			setChildrenTableCreate = con
					.prepareStatement("CREATE TEMPORARY TABLE children "
							+ "(id bigint NOT NULL, CONSTRAINT \"PK_children\" PRIMARY KEY (id)) "
							+ "ON COMMIT DROP;");
			setParentsTableInsert = con
					.prepareStatement("INSERT INTO parents "
							+ "(SELECT equivalentid "
							+ "FROM conexp "
							+ "WHERE id = ? AND endtime IS NULL AND id NOT IN (SELECT id FROM parents));");
			setChildrenTableInsert = con
					.prepareStatement("INSERT INTO children "
							+ "(SELECT equivalentid "
							+ "FROM conexp "
							+ "WHERE id = ? AND endtime IS NULL AND id NOT IN (SELECT id FROM children));");
			setParentsTableAnalyze = con.prepareStatement("ANALYZE parents;");
			setChildrenTableAnalyze = con.prepareStatement("ANALYZE children;");
			setParentsTransitiveclosureParentsInsert = con
					.prepareStatement("INSERT INTO transitiveclosure "
							+ "(sourceid, destinationid, starttime, endtime, directrelation) "
							+ "(SELECT ?, id, now(), null, true FROM parents);");
			setChildrenTransitiveclosureChildrenInsert = con
					.prepareStatement("INSERT INTO transitiveclosure "
							+ "(sourceid, destinationid, starttime, endtime, directrelation) "
							+ "(SELECT id, ?, now(), null, true FROM children);");
			setParentsTransitiveclosureAncestorsInsert = con
					.prepareStatement("INSERT INTO transitiveclosure "
							+ "(sourceid, destinationid, starttime, endtime, directrelation) "
							+ "(SELECT ?, destinationid, now(), null, false "
							+ "FROM transitiveclosure "
							+ "WHERE sourceid IN (SELECT id FROM parents) AND endtime IS NULL);");
			setChildrenTransitiveclosureDescendantsInsert = con
					.prepareStatement("INSERT INTO transitiveclosure "
							+ "(sourceid, destinationid, starttime, endtime, directrelation) "
							+ "(SELECT sourceid, ?, now(), null, false "
							+ "FROM transitiveclosure "
							+ "WHERE destinationid IN (SELECT id FROM children) AND endtime IS NULL);");
			convertDirectToIndirectRelationshipCreatePs = con
					.prepareStatement("INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "(SELECT sourceid, destinationid, now(), NULL, false "
							+ "FROM transitiveclosure "
							+ "WHERE sourceid IN (SELECT id FROM children) AND destinationid IN (SELECT id FROM parents) AND "
							+ "endtime IS NULL AND directrelation = true);");
			convertDirectToIndirectRelationshipRetirePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = now() "
							+ "WHERE sourceid IN (SELECT id FROM children) AND destinationid IN (SELECT id FROM parents) AND "
							+ "endtime IS NULL AND directrelation = true;");
			getExpressionIdPs = con
					.prepareStatement("SELECT id FROM expressions WHERE expression = ?;");
			getExpressionIdTimePs = con
					.prepareStatement("SELECT id FROM expressions WHERE expression = ? "
							+ "AND starttime <= ? AND (? < endtime OR endtime IS NULL);");
			getExpressionPs = con
					.prepareStatement("SELECT expression FROM expressions WHERE id = ?;");
			getExpressionTimePs = con
					.prepareStatement("SELECT expression FROM expressions WHERE id = ? "
							+ "AND starttime <= ? AND (? < endtime OR endtime IS NULL);");
			getDescendantsPs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.destinationid "
							+ "JOIN conexp AS result ON transitiveclosure.sourceid= result.equivalentid "
							+ "WHERE base.endtime IS NULL AND transitiveclosure.endtime IS NULL AND result.endtime IS NULL AND "
							+ "base.id = ?;");
			getDescendantsTimePs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.destinationid "
							+ "JOIN conexp AS result ON transitiveclosure.sourceid = result.equivalentid "
							+ "WHERE base.id = ? AND "
							+ "base.starttime <= ? AND (? < base.endtime OR base.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "result.starttime <= ? AND (? < result.endtime OR result.endtime IS NULL);");
			getChildrenPs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.destinationid "
							+ "JOIN conexp AS result ON transitiveclosure.sourceid = result.equivalentid "
							+ "WHERE transitiveclosure.directrelation = true AND "
							+ "base.endtime IS NULL AND transitiveclosure.endtime IS NULL AND result.endtime IS NULL AND "
							+ "base.id = ?;");
			getChildrenTimePs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.destinationid "
							+ "JOIN conexp AS result ON transitiveclosure.sourceid = result.equivalentid "
							+ "WHERE transitiveclosure.directrelation = true AND "
							+ "base.id = ? AND "
							+ "base.starttime <= ? AND (? < base.endtime OR base.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "result.starttime <= ? AND (? < result.endtime OR result.endtime IS NULL);");
			getAncestorsPs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS result ON transitiveclosure.destinationid = result.equivalentid "
							+ "WHERE base.endtime IS NULL AND transitiveclosure.endtime IS NULL AND result.endtime IS NULL AND "
							+ "base.id = ?;");
			getAncestorsTimePs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS result ON transitiveclosure.destinationid = result.equivalentid "
							+ "WHERE base.id = ? AND "
							+ "base.starttime <= ? AND (? < base.endtime OR base.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "result.starttime <= ? AND (? < result.endtime OR result.endtime IS NULL);");
			getParentsPs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS result ON transitiveclosure.destinationid = result.equivalentid "
							+ "WHERE transitiveclosure.directrelation = true AND "
							+ "base.endtime IS NULL AND transitiveclosure.endtime IS NULL AND result.endtime IS NULL AND "
							+ "base.id = ?;");
			getParentsTimePs = con
					.prepareStatement("SELECT result.id "
							+ "FROM conexp AS base JOIN transitiveclosure ON base.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS result ON transitiveclosure.destinationid = result.equivalentid "
							+ "WHERE transitiveclosure.directrelation = true AND "
							+ "base.id = ? AND "
							+ "base.starttime <= ? AND (? < base.endtime OR base.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "result.starttime <= ? AND (? < result.endtime OR result.endtime IS NULL);");
			getAllExpressionsPs = con
					.prepareStatement("SELECT id, expression FROM expressions WHERE endtime IS NULL;");
			getAllExpressionsTimePs = con
					.prepareStatement("SELECT id, expression FROM expressions WHERE starttime <= ? AND (? < endtime OR endtime IS NULL);");
			isExistingConceptIdPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM concepts WHERE id = ? AND endtime IS NULL;");
			isExistingConceptIdTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM concepts WHERE id = ? AND starttime <= ? AND (? < endtime OR endtime IS NULL);");
			isExistingExpressionIdPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM expressions WHERE id = ? AND endtime IS NULL;");
			isExistingExpressionIdTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM expressions WHERE id = ? AND starttime <= ? AND (? < endtime OR endtime IS NULL);");
			isExistingIdPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM conexp WHERE id = ? AND endtime IS NULL;");
			isExistingIdTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM conexp WHERE id = ? AND starttime <= ? AND (? < endtime OR endtime IS NULL);");
			isSubsumingNotEquivalentPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source JOIN transitiveclosure ON source.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS destination ON transitiveclosure.destinationid = destination.equivalentid "
							+ "WHERE destination.id = ? AND source.id = ? AND "
							+ "source.endtime IS NULL AND transitiveclosure.endtime IS NULL AND destination.endtime IS NULL;");
			isSubsumingNotEquivalentTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source JOIN transitiveclosure ON source.equivalentid = transitiveclosure.sourceid "
							+ "JOIN conexp AS destination ON transitiveclosure.destinationid = destination.equivalentid "
							+ "WHERE destination.id = ? AND source.id = ? AND "
							+ "source.starttime <= ? AND (? < source.endtime OR source.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "destination.starttime <= ? AND (? < destination.endtime OR destination.endtime IS NULL);");
			isEquivalentPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source JOIN conexp AS destination ON source.equivalentid = destination.equivalentid "
							+ "WHERE destination.id = ? AND source.id = ? AND "
							+ "source.endtime IS NULL AND destination.endtime IS NULL;");
			isEquivalentTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source JOIN conexp AS destination ON source.equivalentid = destination.equivalentid "
							+ "WHERE destination.id = ? AND source.id = ? AND "
							+ "source.starttime <= ? AND (? < source.endtime OR source.endtime IS NULL) AND "
							+ "(? = ? OR True) AND "
							+ "destination.starttime <= ? AND (? < destination.endtime OR destination.endtime IS NULL);");
			isSubsumingPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source, transitiveclosure , conexp AS destination "
							+ "WHERE ((source.equivalentid = transitiveclosure.sourceid AND transitiveclosure.destinationid = destination.equivalentid) OR "
							+ "source.equivalentid = destination.equivalentid) AND "
							+ "destination.id = ? AND source.id = ? AND "
							+ "source.endtime IS NULL AND transitiveclosure.endtime IS NULL AND destination.endtime IS NULL;");
			isSubsumingTimePs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist "
							+ "FROM conexp AS source, transitiveclosure , conexp AS destination "
							+ "WHERE ((source.equivalentid = transitiveclosure.sourceid AND transitiveclosure.destinationid = destination.equivalentid) OR "
							+ "source.equivalentid = destination.equivalentid) AND "
							+ "destination.id = ? AND source.id = ? AND "
							+ "source.starttime <= ? AND (? < source.endtime OR source.endtime IS NULL) AND "
							+ "transitiveclosure.starttime <= ? AND (? < transitiveclosure.endtime OR transitiveclosure.endtime IS NULL) AND "
							+ "destination.starttime <= ? AND (? < destination.endtime OR destination.endtime IS NULL);");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	@Override
	public void finalize() throws Throwable {
		// Close the database connection.
		con.close();
		super.finalize();
	}

	@Override
	public ExpressionId storeExpression(final String expression, final Date time)
			throws DataStoreException, ExpressionAlreadyExistsException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		try {
			// Check if the expression already exists in the dbms.
			if (getExpressionId(expression, null) != null) {
				throw new ExpressionAlreadyExistsException("The expression "
						+ expression + " already exists in the data store.");
			}
			// Store the expression in the dbms and set the equivalent id to the
			// expression's own id.
			con.setAutoCommit(false);
			if (sqlTimestamp == null) {
				storeExpressionPs.setString(1, expression);
				storeExpressionPs.executeUpdate();
			} else {
				storeExpressionTimePs.setString(1, expression);
				storeExpressionTimePs.setTimestamp(2, sqlTimestamp);
				storeExpressionTimePs.executeUpdate();
			}
			setEquivalentIdToIdPs.setString(1, expression);
			setEquivalentIdToIdPs.executeUpdate();
			con.commit();
			con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		// Return the assigned expression id.
		return getExpressionId(expression, sqlTimestamp);
	}

	@Override
	public void storeExpressionEquivalence(ExpressionId id,
			ExpressionId equivalentExpressionId) throws DataStoreException,
			NonExistingIdException, RelativeAlreadySetException {
		try {
			// Check if the expression id exists in the dbms.
			if (!isExistingExpressionId(id, null)) {
				throw new NonExistingIdException("The expression id "
						+ id.getId() + " do not exists in the data store.");
			}

			// Check if the expression already has got an equivalence expression
			// id set.
			if (isEquivalentIdSet(id)) {
				throw new RelativeAlreadySetException("The expression with id "
						+ id.getId() + " has already an equivalent id set");
			}

			// Check if the expression already has any parent or child set.
			if (isRelativeSet(id)) {
				throw new RelativeAlreadySetException("The expression with id "
						+ id.getId()
						+ " has already at least one parent or child set.");
			}

			// Check if the equivalent expression id exists in the dbms.
			if (!(isExistingExpressionId(equivalentExpressionId, null) || isExistingConceptId(
					equivalentExpressionId, null))) {
				throw new NonExistingIdException(
						"The equivalent expression id "
								+ equivalentExpressionId.getId()
								+ " do not exists in the data store.");
			}

			// Update the equivalent expression id if the equivalent expression
			// is a concept.
			setEquivalentIdPs.setLong(1, equivalentExpressionId.getId());
			setEquivalentIdPs.setLong(2, id.getId());
			setEquivalentIdPs.executeUpdate();
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	@Override
	public void storeExpressionParentsAndChildren(ExpressionId id,
			Set<ExpressionId> parents, Set<ExpressionId> children)
			throws DataStoreException, NonExistingIdException,
			RelativeAlreadySetException {
		try {
			// Check if the expression's id exists in the dbms.
			if (!isExistingExpressionId(id, null)) {
				throw new NonExistingIdException("The specified id "
						+ id.getId() + " do not exists in the data store.");
			}

			// Check if the expression already has got an equivalence expression
			// id set.
			if (isEquivalentIdSet(id)) {
				throw new RelativeAlreadySetException("The expression with id "
						+ id.getId() + " has already an equivalent id set");
			}

			// Check if the expression already has any parent or child set.
			if (isRelativeSet(id)) {
				throw new RelativeAlreadySetException("The expression with id "
						+ id.getId()
						+ " has already at least one parent or child set.");
			}

			// Check if the parents exists in the dbms.
			for (ExpressionId parentId : parents) {
				if (!isExistingId(parentId, null)) {
					throw new NonExistingIdException("The specified parent id "
							+ parentId.getId()
							+ " do not exists in the data store.");
				}
			}

			// Check if the children exists in the dbms.
			for (ExpressionId childId : children) {
				if (!isExistingId(childId, null)) {
					throw new NonExistingIdException("The specified child id "
							+ childId.getId()
							+ " do not exists in the data store.");
				}
			}

			// Switch of auto commit so all updates are done in the same
			// transaction.
			con.setAutoCommit(false);

			// Create a temporary table for the parents' id.
			setParentsTableCreate.executeUpdate();

			// Create a temporary table for the children's id.
			setChildrenTableCreate.executeUpdate();

			// Insert the parents' id in the temporary table.
			for (ExpressionId parentId : parents) {
				setParentsTableInsert.setLong(1, parentId.getId());
				setParentsTableInsert.executeUpdate();
			}

			// Insert the children's id in the temporary table.
			for (ExpressionId childId : children) {
				setChildrenTableInsert.setLong(1, childId.getId());
				setChildrenTableInsert.executeUpdate();
			}

			// Analyze the temporary table for the parents' id.
			setParentsTableAnalyze.executeUpdate();

			// Analyze the temporary table for the parents' id.
			setChildrenTableAnalyze.executeUpdate();

			// Insert the parents into the transitive closure table.
			setParentsTransitiveclosureParentsInsert.setLong(1, id.getId());
			setParentsTransitiveclosureParentsInsert.executeUpdate();

			// Insert the children into the transitive closure table.
			setChildrenTransitiveclosureChildrenInsert.setLong(1, id.getId());
			setChildrenTransitiveclosureChildrenInsert.executeUpdate();

			// Inserts the ancestors into the transitive closure table.
			setParentsTransitiveclosureAncestorsInsert.setLong(1, id.getId());
			setParentsTransitiveclosureAncestorsInsert.executeUpdate();

			// Inserts the descendants into the transitive closure table.
			setChildrenTransitiveclosureDescendantsInsert
					.setLong(1, id.getId());
			setChildrenTransitiveclosureDescendantsInsert.executeUpdate();

			// Store the current direct relationships as indirect relationships.
			convertDirectToIndirectRelationshipCreatePs.executeUpdate();

			// Retire the current direct relationships
			convertDirectToIndirectRelationshipRetirePs.executeUpdate();

			// Commit all updates
			con.commit();
			// // Switch on auto commit.
			con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	@Override
	public ExpressionId getExpressionId(String expression, Date time)
			throws DataStoreException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		final ExpressionId result;
		try {
			final ResultSet getExpressionIdRs;
			// Look up expression id when no time is given.
			if (sqlTimestamp == null) {
				getExpressionIdPs.setString(1, expression);
				getExpressionIdRs = getExpressionIdPs.executeQuery();
			} else {
				// Look up expression id when a time is given.
				getExpressionIdTimePs.setString(1, expression);
				getExpressionIdTimePs.setTimestamp(2, sqlTimestamp);
				getExpressionIdTimePs.setTimestamp(3, sqlTimestamp);
				getExpressionIdRs = getExpressionIdTimePs.executeQuery();
			}
			// Store the result in the variable.
			if (getExpressionIdRs.next()) {
				result = new ExpressionId(getExpressionIdRs.getLong(1));
			} else {
				result = null;
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	@Override
	public String getExpression(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		final String result;
		try {
			// Check if the expression id exists in the dbms.
			if (!isExistingExpressionId(id, time)) {
				throw new NonExistingIdException("The expression id "
						+ id.getId() + " do not exists in the data store.");
			}
			final ResultSet getExpressionRs;
			// Look up expression when no time is given.
			if (sqlTimestamp == null) {
				getExpressionPs.setLong(1, id.getId());
				getExpressionRs = getExpressionPs.executeQuery();
			} else {
				// Look up expression when a time is given.
				getExpressionTimePs.setLong(1, id.getId());
				getExpressionTimePs.setTimestamp(2, sqlTimestamp);
				getExpressionTimePs.setTimestamp(3, sqlTimestamp);
				getExpressionRs = getExpressionTimePs.executeQuery();
			}
			// Store the result in the variable.
			getExpressionRs.next();
			result = getExpressionRs.getString(1);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	@Override
	public HashSet<ExpressionId> getDescendants(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(getDescendantsPs, getDescendantsTimePs, id, time);
	}

	@Override
	public HashSet<ExpressionId> getChildren(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(getChildrenPs, getChildrenTimePs, id, time);
	}

	@Override
	public HashSet<ExpressionId> getAncestors(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(getAncestorsPs, getAncestorsTimePs, id, time);
	}

	@Override
	public HashSet<ExpressionId> getParents(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(getParentsPs, getParentsTimePs, id, time);
	}

	@Override
	public Set<Expression> getAllExpressions(Date time)
			throws DataStoreException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		final HashSet<Expression> result = new HashSet<Expression>();
		try {
			// Look up all expressions.
			final ResultSet getAllExpressionsRs;
			if (time == null) {
				getAllExpressionsRs = getAllExpressionsPs.executeQuery();
			} else {
				getAllExpressionsTimePs.setTimestamp(1, sqlTimestamp);
				getAllExpressionsTimePs.setTimestamp(2, sqlTimestamp);
				getAllExpressionsRs = getAllExpressionsTimePs.executeQuery();
			}

			// Store the expression in the linked list.
			while (getAllExpressionsRs.next()) {
				result.add(new Expression(new ExpressionId(getAllExpressionsRs
						.getLong(1)), getAllExpressionsRs.getString(2)));
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	@Override
	public boolean isSubsumingNotEquivalent(ExpressionId id1, ExpressionId id2,
			Date time) throws DataStoreException {
		return isSE(id1, id2, time, isSubsumingNotEquivalentPs,
				isSubsumingNotEquivalentTimePs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isEquivalent
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 */
	@Override
	public boolean isEquivalent(ExpressionId id1, ExpressionId id2, Date time)
			throws DataStoreException {
		return isSE(id1, id2, time, isEquivalentPs, isEquivalentTimePs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isSubsuming
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 */
	@Override
	public boolean isSubsuming(ExpressionId id1, ExpressionId id2, Date time)
			throws DataStoreException {
		// Changed to use the two other methods to improve the performance.
		// return isSE(id1, id2, time, isSubsumingPs, isSubsumingTimePs);
		return (isSubsumingNotEquivalent(id1, id2, time) || isEquivalent(id1,
				id2, time));
	}

	@Override
	public boolean isExistingId(ExpressionId id, Date time)
			throws DataStoreException {
		return isExiId(id, time, isExistingIdPs, isExistingIdTimePs);
	}

	/**
	 * Check if one concept or expression is subsuming and/or id equivalent
	 * another concept or expression at a specific time.
	 * 
	 * @param id1
	 *            The id1 of the concept or expression.
	 * @param id2
	 *            The id2 of the concept or expression.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @param isWithoutTimePs
	 *            The <code>PreparedStatement</code> to use if no time is given.
	 * @param isWithTimePs
	 *            The <code>PreparedStatement</code> to use if a time is given.
	 * @return if the concept or expression is subsuming and/or is equivalent to
	 *         another concept or expression.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isSE(ExpressionId id1, ExpressionId id2, Date time,
			PreparedStatement isWithoutTimePs, PreparedStatement isWithTimePs)
			throws DataStoreException {
		final boolean result;
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);

		try {
			final ResultSet isRs;
			// Look up if an id exists when no time is given.
			if (sqlTimestamp == null) {
				isWithoutTimePs.setLong(1, id1.getId());
				isWithoutTimePs.setLong(2, id2.getId());
				isRs = isWithoutTimePs.executeQuery();
			} else {
				// Look up if an id exists when a time is given.
				isWithTimePs.setLong(1, id1.getId());
				isWithTimePs.setLong(2, id2.getId());
				isWithTimePs.setTimestamp(3, sqlTimestamp);
				isWithTimePs.setTimestamp(4, sqlTimestamp);
				isWithTimePs.setTimestamp(5, sqlTimestamp);
				isWithTimePs.setTimestamp(6, sqlTimestamp);
				isWithTimePs.setTimestamp(7, sqlTimestamp);
				isWithTimePs.setTimestamp(8, sqlTimestamp);
				isRs = isWithTimePs.executeQuery();
			}
			isRs.next();
			result = isRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Check if a specified concept id exists in the dbms.
	 * 
	 * @param id
	 *            The specified concept id.
	 * @param time
	 *            The given time.
	 * @return If the concept id exists in the data store or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isExistingConceptId(final ExpressionId id, Date time)
			throws DataStoreException {
		return isExiId(id, time, isExistingConceptIdPs,
				isExistingConceptIdTimePs);
	}

	/**
	 * Check if a specified expression id exists in the dbms.
	 * 
	 * @param id
	 *            The specified expression id.
	 * @param time
	 *            The given time.
	 * @return If the expression id exists in the date store or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isExistingExpressionId(final ExpressionId id, Date time)
			throws DataStoreException {
		return isExiId(id, time, isExistingExpressionIdPs,
				isExistingExpressionIdTimePs);
	}

	/**
	 * Check if the expression already has got an equivalence expression id set.
	 * 
	 * @param id
	 *            The expression's id to check the existence of a equivalence
	 *            expression for.
	 * @return If the expression already has got an equivalence expression id
	 *         set or not.
	 * 
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isEquivalentIdSet(ExpressionId id)
			throws DataStoreException {
		try {
			iSEquivalentIdSetPs.setLong(1, id.getId());
			ResultSet isEquivalentIdSetRs = iSEquivalentIdSetPs.executeQuery();
			isEquivalentIdSetRs.next();
			return isEquivalentIdSetRs.getBoolean(1);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/**
	 * Check if the expression already has any parent or child set.
	 * 
	 * @param id
	 *            The expression's id to check the existence of any parent or
	 *            child for.
	 * @return If the expression already has any parent or child set or not.
	 * 
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isRelativeSet(ExpressionId id) throws DataStoreException {
		try {
			isRelativeSetPs.setLong(1, id.getId());
			isRelativeSetPs.setLong(2, id.getId());
			ResultSet isRelativeSetRs = isRelativeSetPs.executeQuery();
			isRelativeSetRs.next();
			return isRelativeSetRs.getBoolean(1);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/**
	 * 
	 * Look up relatives from the dbms using <code>PreparedStatement</code> .
	 * 
	 * @param getWithoutTimePs
	 *            The <code>PreparedStatement</code> to use if no time is given.
	 * @param getWithTimePs
	 *            The <code>PreparedStatement</code> to use if a time is given.
	 * @param id
	 *            The expression id to look up the relatives to.
	 * @param time
	 *            The given time.
	 * @return The expression ids of the relatives.
	 * @throws NonExistingIdException
	 *             The expression id do not exists in the dbms.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private HashSet<ExpressionId> getRelative(
			PreparedStatement getWithoutTimePs,
			PreparedStatement getWithTimePs, ExpressionId id, Date time)
			throws NonExistingIdException, DataStoreException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		final HashSet<ExpressionId> result = new HashSet<ExpressionId>();
		try {
			// Check if the id exists in the dbms.
			if (!isExistingId(id, time)) {
				throw new NonExistingIdException("The specified id "
						+ id.getId() + " do not exists in the data store.");
			}
			final ResultSet rs;
			// Look up the relatives if no time is given.
			if (sqlTimestamp == null) {
				getWithoutTimePs.setLong(1, id.getId());
				rs = getWithoutTimePs.executeQuery();
				// Look up the relatives if a time is given.
			} else {
				getWithTimePs.setLong(1, id.getId());
				getWithTimePs.setTimestamp(2, sqlTimestamp);
				getWithTimePs.setTimestamp(3, sqlTimestamp);
				getWithTimePs.setTimestamp(4, sqlTimestamp);
				getWithTimePs.setTimestamp(5, sqlTimestamp);
				getWithTimePs.setTimestamp(6, sqlTimestamp);
				getWithTimePs.setTimestamp(7, sqlTimestamp);
				rs = getWithTimePs.executeQuery();
			}
			// Store the result.
			while (rs.next()) {
				result.add(new ExpressionId(rs.getLong(1)));
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Check if an id exist as an id for a concept or expression or both
	 * depending on the used <code>PreparedStatement</code>.
	 * 
	 * @param id
	 *            The id to check the existence for.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the
	 *            current time.
	 * @param isWithoutTimePs
	 *            The <code>PreparedStatement</code> to use if no time is given.
	 * @param isWithTimePs
	 *            The <code>PreparedStatement</code> to use if a time is given.
	 * 
	 * @return If the id exist or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.F
	 */
	private boolean isExiId(ExpressionId id, Date time,
			PreparedStatement isWithoutTimePs, PreparedStatement isWithTimePs)
			throws DataStoreException {
		final Timestamp sqlTimestamp = (time != null ? new Timestamp(
				time.getTime()) : null);
		final boolean result;
		try {
			final ResultSet isExistingIdRs;
			// Look up if an id exists when no time is given.
			if (sqlTimestamp == null) {
				isWithoutTimePs.setLong(1, id.getId());
				isExistingIdRs = isWithoutTimePs.executeQuery();
			} else {
				// Look up if an id exists when a time is given.
				isWithTimePs.setLong(1, id.getId());
				isWithTimePs.setTimestamp(2, sqlTimestamp);
				isWithTimePs.setTimestamp(3, sqlTimestamp);
				isExistingIdRs = isWithTimePs.executeQuery();
			}
			isExistingIdRs.next();
			result = isExistingIdRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

}