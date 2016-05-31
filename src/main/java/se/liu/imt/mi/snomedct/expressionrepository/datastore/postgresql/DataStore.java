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

import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyDefined;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyExistsException;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * An implementation of the <code>DataStore</code> interface for the PostgreSQL database management system.
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class DataStore implements se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore {

	/**
	 * The connection to the PostgreSQL database management system, dbms, containing the expression database.
	 */
	protected Connection con;

	/**
	 * A <code>PreparedStatement</code> which checks if an expression already exists in the dbms.
	 */
	private final PreparedStatement isExistingExpressionPs;
	/**
	 * A <code>PreparedStatement</code> that store an a expression with a timestamp in the dbms.
	 */
	private final PreparedStatement setExpressionPs;

	/**
	 * A <code>PreparedStatement</code> that store the id in an equivalent expression group if there is no suitable
	 * equivalent expression group already existing in the dbms.
	 */
	private final PreparedStatement setEquivalentIdGroupPs;
	/**
	 * A <code>PreparedStatement</code> that store the id in the equivalent expression group in the dbms.
	 */
	private final PreparedStatement setEquivalentIdPs;

	/**
	 * A <code>PreparedStatement</code> that create a temporary table for the parent(s) to the concept or expression.
	 */
	private final PreparedStatement storeRelativesCreateTableParentsPs;
	/**
	 * A <code>PreparedStatement</code> that create a temporary table for the child(ren) to the concept or expression.
	 */
	private final PreparedStatement storeRelativesCreateTableChildrenPs;
	/**
	 * A <code>PreparedStatement</code> that store the parent(s) to the to the concept or expression in the temporary
	 * table.
	 */
	private final PreparedStatement storeRelativesInsertIntoTableParentsPs;
	/**
	 * A <code>PreparedStatement</code> that store the child(ren) to the to the concept or expression in the temporary
	 * table.
	 */
	private final PreparedStatement storeRelativesInsertIntoTableChildrenPs;
	/**
	 * A <code>PreparedStatement</code> that analyze the temporary table for the parent(s) to the concept or expression.
	 */
	private final PreparedStatement storeRelativesAnalyzeTableParentsPs;
	/**
	 * A <code>PreparedStatement</code> that analyze the temporary table for the child(ren) to the concept or
	 * expression.
	 */
	private final PreparedStatement storeRelativesAnalyzeTableChildrenPs;
	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the parent relationship(s) for the concept or
	 * expression that has it's parent(s) set.
	 */
	private final PreparedStatement storeRelativesParentsExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the parent relationship(s) for the concept or expression that has
	 * it's parent(s) set.
	 */
	private final PreparedStatement storeRelativesParentsInsertPs;
	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the child relationship(s) for the concept or
	 * expression that has it's child(ren) set.
	 */
	private final PreparedStatement storeRelativesChildrenExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the child relationship(s) for the concept expression that has it's
	 * child(ren) set.
	 */
	private final PreparedStatement storeRelativesChildrenInsertPs;
	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the relevant ancestors for the expression that has
	 * it's parent(s) set.
	 */
	private final PreparedStatement storeRelativesAncestorsExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the relevant ancestors for the expression that has it's parent(s)
	 * set.
	 */
	private final PreparedStatement storeRelativesAncestorsInsertPs;
	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the relevant descendants for the expression that has
	 * it's child(ren) set.
	 */
	private final PreparedStatement storeRelativesDescendantsExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the relevant links between the descendants for the expression that
	 * has it's child(ren) set.
	 */
	private final PreparedStatement storeRelativesDescendantsInsertPs;
	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the relevant links between the parents and the
	 * children for the expression that has it's parent(s) and child(ren) set.
	 */
	private final PreparedStatement storeRelativesLinkParentsAndChildrenExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the relevant links between the parents and the children for the
	 * expression that has it's parent(s) and child(ren) set.
	 */
	private final PreparedStatement storeRelativesLinkParentsAndChildrenInsertPs;

	/**
	 * A <code>PreparedStatement</code> that extend the endtime for the relevant links between the ancestors and the
	 * descendants for the expression that has it's parent(s) and child(ren) set.
	 */
	private final PreparedStatement storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that insert the relevant links between the ancestors and the descendants for the
	 * expression that has it's parent(s) and child(ren) set.
	 */
	private final PreparedStatement storeRelativesLinkAncestorsAndDescendantsInsertPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's id given the expression itself at a specific time
	 * from the dbms.
	 */
	private final PreparedStatement getExpressionIdPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression given the expression's id at a specific time from
	 * the dbms.
	 */
	private final PreparedStatement getExpressionPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's ancestors at a specific time from the dbms.
	 */
	private final PreparedStatement getAncestorsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's descendants at a specific time from the dbms.
	 */
	private final PreparedStatement getDescendantsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's parents at a specific time from the dbms.
	 */
	private final PreparedStatement getParentsPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve an expression's children at a specific time from the dbms.
	 */
	private final PreparedStatement getChildrenPs;

	/**
	 * A <code>PreparedStatement</code> that retrieve all expressions at a specific time from the dbms.
	 */
	private final PreparedStatement getAllExpressionsPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression subsumes but is not equivalent to
	 * another concept or expression at a specific time.
	 */
	private final PreparedStatement isSubsumingNotEquivalentPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression is equivalent to another concept or
	 * expression at a specific time.
	 */
	private final PreparedStatement isEquivalentPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an concept or expression id exists at the current time in the
	 * dbms.
	 */
	private final PreparedStatement isExistingIdPs;

	/**
	 * A <code>PreparedStatement</code> which checks if an expression id exists in the dbms.
	 */
	private final PreparedStatement isExistingExpressionIdPs;

	/**
	 * A <code>PreparedStatement</code> that check if an equivalence with a future start already has been set for the
	 * expression in the dbms.
	 */
	private final PreparedStatement isFutureEquivalentSetPs;

	/**
	 * A <code>PreparedStatement</code> that check if parent(s) and/or child(ren) with a future start already has been
	 * set for the expression in the dbms.
	 */
	private final PreparedStatement isFutureRelativeSetPs;

	/**
	 * A <code>PreparedStatement</code> that create new relationships for the concepts in an equivalence group if the
	 * current relationships for the concepts with the relationships in the group is going to be inactivated.
	 */
	private final PreparedStatement inactivateRelativesCreateNewRelationshipsForEquivalencePs;
	/**
	 * A <code>PreparedStatement</code> that create new direct relationships between the parent(s) and child(ren) of the
	 * concept which is going to be retired if no other concept from an equivalence group is used instead.
	 */
	private final PreparedStatement inactivateRelativesCreateNewDirectRelationshipsPs;
	/**
	 * A <code>PreparedStatement</code> that set the end time to the relationships that is going to be retired.
	 */
	private final PreparedStatement inactivateRelativesSetEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that delete relationships with the same starttime as the time the retirement is
	 * done.
	 */
	private final PreparedStatement inactivateRelativesDeleteWithCurrentStartTimePs;
	/**
	 * A <code>PreparedStatement</code> that set the end time to the second last expression equivalence in the group.
	 * The last expression will be inactivated by another statement, so there is no longer any use to include this
	 * expression in the group.
	 */
	private final PreparedStatement inactivateEquivalenceGroupSetEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that delete the second last expression equivalence in the group. The last
	 * expression will be inactivated by another statement, so there is no longer any use to include this expression in
	 * the group.
	 */
	private final PreparedStatement inactivateEquivalenceGroupDeleteWithCurrentStartTimePs;
	/**
	 * A <code>PreparedStatement</code> that set the end time to the expression equivalence that is going to be retired.
	 */
	private final PreparedStatement inactivateEquivalenceSetEndtimePs;
	/**
	 * A <code>PreparedStatement</code> that delete the expression equivalence with the same starttime as the time the
	 * retirement is done.
	 */
	private final PreparedStatement inactivateEquivalenceDeleteWithCurrentStartTimePs;

	/**
	 * Creates a data store API and set up a connection to the PostgreSQL database management system containing the
	 * expression database.
	 * 
	 * @param url
	 *            The URL for the database connection.
	 * @param userName
	 *            The user name for the database connection.
	 * @param password
	 *            The user password for the database connection.
	 * @throws DataStoreException
	 *             Thrown if there is a problem with the dbms or the connection to the dbms.
	 */
	public DataStore(final String url, final String userName, final String password) throws DataStoreException {
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

		// Create the prepared statements.
		try {
			isExistingExpressionPs = con
					.prepareStatement("SELECT Count(*) > 0 as exist FROM expressions WHERE expression = ?;");
			setExpressionPs = con
					.prepareStatement("INSERT INTO expressions (expression, starttime) VALUES (?, ?) RETURNING id;");

			setEquivalentIdGroupPs = con.prepareStatement("INSERT INTO equivalents (id, starttime) SELECT ?, ? "
					+ "WHERE (SELECT Count(*) = 0 FROM equivalents WHERE id = ? AND starttime <= ? AND ? < endtime)");
			setEquivalentIdPs = con.prepareStatement("INSERT INTO equivalents (id, starttime, equivalentid) "
					+ "SELECT ?, ?, equivalentid FROM equivalents WHERE id = ? AND starttime <= ? AND ? < endtime ");

			storeRelativesCreateTableParentsPs = con
					.prepareStatement("CREATE TEMPORARY TABLE parents_insert (id bigint NOT NULL) ON COMMIT DROP;");
			storeRelativesCreateTableChildrenPs = con
					.prepareStatement("CREATE TEMPORARY TABLE children_insert (id bigint NOT NULL) ON COMMIT DROP;");
			storeRelativesInsertIntoTableParentsPs = con
					.prepareStatement("INSERT INTO parents_insert (id) VALUES (?);");
			storeRelativesInsertIntoTableChildrenPs = con
					.prepareStatement("INSERT INTO children_insert (id) VALUES (?);");
			storeRelativesAnalyzeTableParentsPs = con.prepareStatement("ANALYZE parents_insert;");
			storeRelativesAnalyzeTableChildrenPs = con.prepareStatement("ANALYZE children_insert;");
			storeRelativesParentsExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp " + "FROM ("
							+ "SELECT DISTINCT ? AS sourceid, parents_insert.id AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, true AS directrelation FROM parents_insert "
							+ ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesParentsInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT ? AS sourceid, parents_insert.id AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, true AS directrelation FROM parents_insert "
							+ ") AS insert_rows " + "LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.starttime = transitiveclosure.starttime AND insert_rows.endtime = transitiveclosure.endtime AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation "
							+ "WHERE transitiveclosure.sourceid IS NULL;");
			storeRelativesChildrenExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp " + "FROM ("
							+ "SELECT DISTINCT children_insert.id AS sourceid, ? AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, true AS directrelation FROM children_insert "
							+ ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesChildrenInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT children_insert.id AS sourceid, ? AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, true AS directrelation FROM children_insert "
							+ ") AS insert_rows " + "LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.starttime = transitiveclosure.starttime AND insert_rows.endtime = transitiveclosure.endtime AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation "
							+ "WHERE transitiveclosure.sourceid IS NULL;");
			storeRelativesAncestorsExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp FROM ("
							+ "SELECT DISTINCT ? AS sourceid, ancestors.destinationid AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS ancestors "
							+ "WHERE ancestors.sourceid IN (SELECT id FROM parents_insert) AND ancestors.starttime <= ?::timestamp AND ?::timestamp < ancestors.endtime "
							+ ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesAncestorsInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT ? AS sourceid, ancestors.destinationid AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS ancestors "
							+ "WHERE ancestors.sourceid IN (SELECT id FROM parents_insert) AND ancestors.starttime <= ?::timestamp AND ?::timestamp < ancestors.endtime "
							+ ") AS insert_rows LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation AND insert_rows.starttime >= transitiveclosure.starttime AND transitiveclosure.endtime = 'infinity' "
							+ "WHERE transitiveclosure.sourceid IS NULL;");
			storeRelativesDescendantsExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp FROM ("
							+ "SELECT DISTINCT descendants.sourceid AS sourceid, ? AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS descendants "
							+ "WHERE descendants.destinationid IN (SELECT id FROM children_insert) AND descendants.starttime <= ?::timestamp AND ?::timestamp < descendants.endtime "
							+ ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesDescendantsInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT descendants.sourceid AS sourceid, ? AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS descendants "
							+ "WHERE descendants.destinationid IN (SELECT id FROM children_insert) AND descendants.starttime <= ?::timestamp AND ?::timestamp < descendants.endtime "
							+ ") AS insert_rows LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation AND insert_rows.starttime >= transitiveclosure.starttime AND transitiveclosure.endtime = 'infinity' "
							+ "WHERE transitiveclosure.sourceid IS NULL;");
			storeRelativesLinkParentsAndChildrenExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp FROM ("
							+ "SELECT DISTINCT children_insert.id AS sourceid, parents_insert.id AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM children_insert, parents_insert " + ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesLinkParentsAndChildrenInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT children_insert.id AS sourceid, parents_insert.id AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM children_insert, parents_insert "
							+ ") AS insert_rows LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation AND insert_rows.starttime >= transitiveclosure.starttime AND transitiveclosure.endtime = 'infinity' "
							+ "WHERE transitiveclosure.sourceid IS NULL;");
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs = con
					.prepareStatement("UPDATE transitiveclosure SET endtime = 'infinity'::timestamp FROM ("
							+ "SELECT DISTINCT descendants.sourceid AS sourceid, ancestors.destinationid AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS descendants, transitiveclosure AS ancestors "
							+ "WHERE descendants.destinationid IN (SELECT id FROM children_insert) AND ancestors.sourceid IN (SELECT ID FROM parents_insert) AND "
							+ "descendants.starttime <= ?::timestamp AND ?::timestamp < descendants.endtime AND ancestors.starttime <= ?::timestamp AND ?::timestamp < ancestors.endtime "
							+ ") AS insert_rows "
							+ "WHERE transitiveclosure.sourceid = insert_rows.sourceid AND transitiveclosure.destinationid = insert_rows.destinationid AND "
							+ "transitiveclosure.endtime = insert_rows.starttime AND transitiveclosure.directrelation = insert_rows.directrelation;");
			storeRelativesLinkAncestorsAndDescendantsInsertPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT insert_rows.sourceid, insert_rows.destinationid, insert_rows.starttime, insert_rows.endtime, insert_rows.directrelation FROM ("
							+ "SELECT DISTINCT descendants.sourceid AS sourceid, ancestors.destinationid AS destinationid, ?::timestamp AS starttime, 'infinity'::timestamp AS endtime, false AS directrelation "
							+ "FROM transitiveclosure AS descendants, transitiveclosure AS ancestors "
							+ "WHERE descendants.destinationid IN (SELECT id FROM children_insert) AND ancestors.sourceid IN (SELECT ID FROM parents_insert) AND "
							+ "descendants.starttime <= ?::timestamp AND ?::timestamp < descendants.endtime AND ancestors.starttime <= ?::timestamp AND ?::timestamp < ancestors.endtime "
							+ ") AS insert_rows LEFT JOIN transitiveclosure ON "
							+ "insert_rows.sourceid = transitiveclosure.sourceid AND insert_rows.destinationid = transitiveclosure.destinationid AND "
							+ "insert_rows.directrelation = transitiveclosure.directrelation AND insert_rows.starttime >= transitiveclosure.starttime AND transitiveclosure.endtime = 'infinity' "
							+ "WHERE transitiveclosure.sourceid IS NULL;");

			getExpressionIdPs = con.prepareStatement("SELECT id FROM expressions WHERE expression = ? "
					+ "AND starttime <= ? AND (? < endtime OR endtime IS NULL);");

			getExpressionPs = con.prepareStatement("SELECT expression FROM expressions WHERE id = ? "
					+ "AND starttime <= ? AND (? < endtime OR endtime IS NULL);");

			getAncestorsPs = con.prepareStatement("SELECT destination_coneqv.id2 AS id "
					+ "FROM coneqv AS source_coneqv JOIN transitiveclosure ON source_coneqv.id2 = transitiveclosure.sourceid "
					+ "JOIN coneqv AS destination_coneqv ON transitiveclosure.destinationid = destination_coneqv.id1 "
					+ "WHERE source_coneqv.id1 = ? AND "
					+ "source_coneqv.starttime <= ?::timestamp AND ?::timestamp < source_coneqv.endtime AND "
					+ "transitiveclosure.starttime <= ?::timestamp AND ?::timestamp < transitiveclosure.endtime AND "
					+ "destination_coneqv.starttime <= ?::timestamp AND ?::timestamp < destination_coneqv.endtime;");

			getDescendantsPs = con.prepareStatement("SELECT source_coneqv.id2 AS id "
					+ "FROM coneqv AS source_coneqv JOIN transitiveclosure ON source_coneqv.id1 = transitiveclosure.sourceid "
					+ "JOIN coneqv AS destination_coneqv ON transitiveclosure.destinationid = destination_coneqv.id2 "
					+ "WHERE destination_coneqv.id1 = ? AND "
					+ "source_coneqv.starttime <= ?::timestamp AND ?::timestamp < source_coneqv.endtime AND "
					+ "transitiveclosure.starttime <= ?::timestamp AND ?::timestamp < transitiveclosure.endtime AND "
					+ "destination_coneqv.starttime <= ?::timestamp AND ?::timestamp < destination_coneqv.endtime;");

			getParentsPs = con.prepareStatement("SELECT destination_coneqv.id2 AS id "
					+ "FROM coneqv AS source_coneqv JOIN transitiveclosure ON source_coneqv.id2 = transitiveclosure.sourceid "
					+ "JOIN coneqv AS destination_coneqv ON transitiveclosure.destinationid = destination_coneqv.id1 "
					+ "WHERE source_coneqv.id1 = ? AND directrelation = true AND "
					+ "source_coneqv.starttime <= ?::timestamp AND ?::timestamp < source_coneqv.endtime AND "
					+ "transitiveclosure.starttime <= ?::timestamp AND ?::timestamp < transitiveclosure.endtime AND "
					+ "destination_coneqv.starttime <= ?::timestamp AND ?::timestamp < destination_coneqv.endtime;");

			getChildrenPs = con.prepareStatement("SELECT source_coneqv.id2 AS id "
					+ "FROM coneqv AS source_coneqv JOIN transitiveclosure ON source_coneqv.id1 = transitiveclosure.sourceid "
					+ "JOIN coneqv AS destination_coneqv ON transitiveclosure.destinationid = destination_coneqv.id2 "
					+ "WHERE destination_coneqv.id1 = ? AND directrelation = true AND "
					+ "source_coneqv.starttime <= ?::timestamp AND ?::timestamp < source_coneqv.endtime AND "
					+ "transitiveclosure.starttime <= ?::timestamp AND ?::timestamp < transitiveclosure.endtime AND "
					+ "destination_coneqv.starttime <= ?::timestamp AND ?::timestamp < destination_coneqv.endtime;");

			getAllExpressionsPs = con
					.prepareStatement("SELECT id, expression FROM expressions WHERE starttime <= ? AND ? < endtime;");

			isSubsumingNotEquivalentPs = con.prepareStatement("SELECT Count(*) > 0 AS exist "
					+ "FROM coneqv AS source_coneqv JOIN transitiveclosure ON source_coneqv.id2 = transitiveclosure.sourceid "
					+ "JOIN coneqv AS destination_coneqv ON transitiveclosure.destinationid = destination_coneqv.id2 "
					+ "WHERE source_coneqv.id1 = ? AND destination_coneqv.id1 = ? AND "
					+ "source_coneqv.starttime <= ?::timestamp AND ?::timestamp < source_coneqv.endtime AND "
					+ "transitiveclosure.starttime <= ?::timestamp AND ?::timestamp < transitiveclosure.endtime AND "
					+ "destination_coneqv.starttime <= ?::timestamp AND ?::timestamp < destination_coneqv.endtime;");

			isEquivalentPs = con.prepareStatement("SELECT Count(*) > 0 AS exist FROM coneqv "
					+ "WHERE id1 = ? AND id2 = ? AND starttime <= ?::timestamp AND ?::timestamp < endtime;");

			isExistingIdPs = con.prepareStatement("SELECT Count(*) >= 1 AS exist FROM "
					+ "(SELECT id, starttime, endtime FROM concepts UNION SELECT id, starttime, endtime FROM expressions) AS inn "
					+ "WHERE id = ? AND starttime <= ? AND ? < endtime;");

			isExistingExpressionIdPs = con.prepareStatement(
					"SELECT Count(*) >= 1 AS exist FROM expressions WHERE id = ? AND starttime <= ? AND ? < endtime;");

			isFutureEquivalentSetPs = con
					.prepareStatement("SELECT Count(*) >= 1 AS exist FROM equivalents WHERE id = ? AND starttime > ?;");

			isFutureRelativeSetPs = con.prepareStatement(
					"SELECT Count(*) >= 1 AS exist FROM transitiveclosure WHERE (sourceid = ? OR destinationid = ?) AND starttime > ?;");

			inactivateRelativesCreateNewRelationshipsForEquivalencePs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT CASE sourceid WHEN ids.oldid THEN ids.newid ELSE sourceid END AS sourceid, "
							+ "CASE destinationid WHEN ids.oldid THEN ids.newid ELSE destinationid END AS destinationid, "
							+ "? AS starttime, endtime, directrelation " + "FROM transitiveclosure JOIN "
							+ "(SELECT id1 AS oldid, max(id2) AS newid FROM eqv "
							+ "WHERE id1 = ? AND starttime <= ? AND ? < endtime GROUP BY id1) AS ids "
							+ "ON transitiveclosure.sourceid = ids.oldid OR transitiveclosure.destinationid = ids.oldid "
							+ "WHERE transitiveclosure.starttime <= ? AND ? < transitiveclosure.endtime;");
			inactivateRelativesCreateNewDirectRelationshipsPs = con.prepareStatement(
					"INSERT INTO transitiveclosure (sourceid, destinationid, starttime, endtime, directrelation) "
							+ "SELECT children.sourceid, parents.destinationid, "
							+ "? AS starttime, LEAST(children.endtime, parents.endtime) AS endtime, "
							+ "true AS directrelation "
							+ "FROM transitiveclosure AS children JOIN transitiveclosure AS parents ON children.destinationid = parents.sourceid "
							+ "WHERE children.directrelation = TRUE AND parents.directrelation = TRUE AND children.destinationid = ? AND "
							+ "children.starttime <= ? AND parents.starttime <= ? AND children.endtime > ? AND parents.endtime > ? AND "
							+ "(SELECT Count(*) = 0 FROM eqv WHERE id1 = ? AND starttime <= ? AND endtime > ?);");
			inactivateRelativesSetEndtimePs = con.prepareStatement("UPDATE transitiveclosure SET endtime = ? "
					+ "WHERE (sourceid = ? OR destinationid = ?) AND starttime < ? AND endtime > ?;");
			inactivateRelativesDeleteWithCurrentStartTimePs = con.prepareStatement(
					"DELETE FROM transitiveclosure " + "WHERE starttime = ? AND (sourceid = ? OR destinationid = ?);");
			inactivateEquivalenceGroupSetEndtimePs = con.prepareStatement("UPDATE equivalents SET endtime = ? FROM eqv "
					+ "WHERE equivalents.id = eqv.id2 AND equivalents.starttime < ? AND ? < equivalents.endtime AND "
					+ "eqv.id1 = ? AND eqv.starttime <= ? AND ? < eqv.endtime AND "
					+ "(SELECT Count(*) = 1 FROM eqv WHERE id1 = ? AND starttime <= ? AND ? < endtime);");
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs = con.prepareStatement(
					"DELETE FROM equivalents " + "WHERE starttime = ? AND id IN " + "(SELECT id2 FROM eqv "
							+ "WHERE eqv.id1 = ? AND eqv.starttime <= ? AND ? < eqv.endtime AND "
							+ "(SELECT Count(*) = 1 FROM eqv WHERE id1 = ? AND starttime <= ? AND ? < endtime))");
			inactivateEquivalenceSetEndtimePs = con.prepareStatement(
					"UPDATE equivalents SET endtime = ? WHERE id = ? AND starttime < ? AND endtime > ?;");
			inactivateEquivalenceDeleteWithCurrentStartTimePs = con
					.prepareStatement("DELETE FROM equivalents WHERE (starttime = ? AND id = ?);");

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#storeExpression(java.lang.String,
	 * java.util.Date)
	 */
	@Override
	public ExpressionId storeExpression(final String expression, final Date time)
			throws DataStoreException, ExpressionAlreadyExistsException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		final ExpressionId result;
		try {
			// Check if the expression already exists in the dbms.
			isExistingExpressionPs.setString(1, expression);
			final ResultSet isExistingExpressionRs = isExistingExpressionPs.executeQuery();
			isExistingExpressionRs.next();
			if (isExistingExpressionRs.getBoolean("exist")) {
				throw new ExpressionAlreadyExistsException(
						"The expression " + expression + " already exists in the data store.");
			}
			// Store the expression in the dbms.
			setExpressionPs.setString(1, expression);
			setExpressionPs.setTimestamp(2, sqlTimestamp);
			final ResultSet setExpressionRs = setExpressionPs.executeQuery();
			// Return the assigned expression id.
			setExpressionRs.next();
			result = new ExpressionId(setExpressionRs.getLong("id"));
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#storeExpressionEquivalence(se.liu.imt.mi.snomedct
	 * .expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 * 
	 */
	@Override
	public void storeExpressionEquivalence(ExpressionId id, ExpressionId equivalentExpressionId, Date time)
			throws DataStoreException, NonExistingIdException, ExpressionAlreadyDefined {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		try {
			// Check if the expression id exists in the dbms.
			if (!isExistingExpressionId(id, sqlTimestamp)) {
				throw new NonExistingIdException("The expression id " + id.getId()
						+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
			}
			// Check if the equivalent expression id exists in the dbms.
			if (!isExistingId(equivalentExpressionId, sqlTimestamp)) {
				throw new NonExistingIdException("The id " + equivalentExpressionId.getId()
						+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
			}
			// Check if the id and equivalentExpressionId are equal.
			if (id.equals(equivalentExpressionId)) {
				throw new NonExistingIdException("It can not be stored in the data store that the expression with id "
						+ id.getId() + " is equivalent to itself.");
			}

			// Check if an equivalence with a future start time already has been set.
			if (isFutureEquivalentSet(id, sqlTimestamp)) {
				throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
						+ " has already an equivalent id set with a start time in the future.");
			}
			// Check if parent(s) and/or child(ren) with a future start time already has been set.
			if (isFutureRelativeSet(id, sqlTimestamp)) {
				throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
						+ " has already parent(s) and/or child(ren) set with a start time in the future.");
			}

			// Switch of auto commit so all updates are done in the same transaction.
			con.setAutoCommit(false);

			// Inactivate the definition for the expression to set the equivalence for.
			inactivateExpressionDefinition(id, sqlTimestamp);
			// Store the id in an equivalent expression group if there is no suitable equivalent expression group
			// already existing in the dbms.
			setEquivalentIdGroupPs.setLong(1, equivalentExpressionId.getId());
			setEquivalentIdGroupPs.setTimestamp(2, sqlTimestamp);
			setEquivalentIdGroupPs.setLong(3, equivalentExpressionId.getId());
			setEquivalentIdGroupPs.setTimestamp(4, sqlTimestamp);
			setEquivalentIdGroupPs.setTimestamp(5, sqlTimestamp);
			setEquivalentIdGroupPs.executeUpdate();

			// Store the id in the equivalent expression group in the dbms.
			setEquivalentIdPs.setLong(1, id.getId());
			setEquivalentIdPs.setTimestamp(2, sqlTimestamp);
			setEquivalentIdPs.setLong(3, equivalentExpressionId.getId());
			setEquivalentIdPs.setTimestamp(4, sqlTimestamp);
			setEquivalentIdPs.setTimestamp(5, sqlTimestamp);
			setEquivalentIdPs.executeUpdate();

			// Commit all updates
			con.commit();
			// Switch on auto commit.
			con.setAutoCommit(true);

		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#storeExpressionParentsAndChildren(se.liu.imt.mi.
	 * snomedct.expressionrepository.datatypes.ExpressionId, java.util.Set, java.util.Set, java.util.Date)
	 */
	@Override
	public void storeExpressionParentsAndChildren(ExpressionId id, Set<ExpressionId> parents,
			Set<ExpressionId> children, Date time)
					throws DataStoreException, NonExistingIdException, ExpressionAlreadyDefined {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);

		// Check if the expression's id exists in the dbms.
		if (!isExistingExpressionId(id, sqlTimestamp)) {
			throw new NonExistingIdException("The specified id " + id.getId()
					+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
		}

		// Check if an equivalence with a future start time already has been set.
		if (isFutureEquivalentSet(id, sqlTimestamp)) {
			throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
					+ " has already an equivalent id set with a start time in the future.");
		}
		// Check if parent(s) and/or child(ren) with a future start time already has been set.
		if (isFutureRelativeSet(id, sqlTimestamp)) {
			throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
					+ " has already parent(s) and/or child(ren) set with a start time in the future.");
		}

		if (parents != null) {
			// Check if the parents exists in the dbms.
			for (ExpressionId parentId : parents) {
				if (!isExistingId(parentId, sqlTimestamp)) {
					throw new NonExistingIdException("The specified parent id " + parentId.getId()
							+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
				}
			}
			// Check if any of the parents have an equivalence with a future start time set.
			for (ExpressionId parentId : parents) {
				if (isFutureEquivalentSet(parentId, sqlTimestamp)) {
					throw new ExpressionAlreadyDefined("The parent expression with id " + parentId.getId()
							+ " has already an equivalent id set with a start time in the future.");
				}
				if (isFutureRelativeSet(parentId, sqlTimestamp)) {
					throw new ExpressionAlreadyDefined("The parent expression with id " + parentId.getId()
							+ " has already parent(s) and/or child(ren) set with a start time in the future.");
				}
			}
		}

		if (children != null) {
			// Check if the children exists in the dbms.
			for (ExpressionId childId : children) {
				if (!isExistingId(childId, sqlTimestamp)) {
					throw new NonExistingIdException("The specified child id " + childId.getId()
							+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
				}
			}
			// Check if any of the children have an equivalence with a future start time set.
			for (ExpressionId childId : children) {
				if (isFutureEquivalentSet(childId, sqlTimestamp)) {
					throw new ExpressionAlreadyDefined("The child expression with id " + childId.getId()
							+ " has already an equivalent id set with a start time in the future.");
				}
				if (isFutureRelativeSet(childId, sqlTimestamp)) {
					throw new ExpressionAlreadyDefined("The child expression with id " + childId.getId()
							+ " has already parent(s) and/or child(ren) set with a start time in the future.");
				}
			}
		}

		try {
			// Switch of auto commit so all updates are done in the same transaction.
			con.setAutoCommit(false);

			// Inactivate the definition for the expression to set the equivalence for.
			inactivateExpressionDefinition(id, sqlTimestamp);

			// Store the parent(s) in a temporary table.
			storeRelativesCreateTableParentsPs.executeUpdate();
			if (parents != null) {
				for (ExpressionId parentId : parents) {
					storeRelativesInsertIntoTableParentsPs.setLong(1, parentId.getId());
					storeRelativesInsertIntoTableParentsPs.executeUpdate();
				}
			}
			storeRelativesAnalyzeTableParentsPs.executeUpdate();

			// Store the child(ren) in a temporary table.
			storeRelativesCreateTableChildrenPs.executeUpdate();
			if (children != null) {
				for (ExpressionId childId : children) {
					storeRelativesInsertIntoTableChildrenPs.setLong(1, childId.getId());
					storeRelativesInsertIntoTableChildrenPs.executeUpdate();
				}
			}
			storeRelativesAnalyzeTableChildrenPs.executeUpdate();

			// Store the parents in the transitive closure table.
			storeRelativesParentsExtendEndtimePs.setLong(1, id.getId());
			storeRelativesParentsExtendEndtimePs.setTimestamp(2, sqlTimestamp);
			storeRelativesParentsExtendEndtimePs.executeUpdate();
			storeRelativesParentsInsertPs.setLong(1, id.getId());
			storeRelativesParentsInsertPs.setTimestamp(2, sqlTimestamp);
			storeRelativesParentsInsertPs.executeUpdate();

			// Store the children in the transitive closure table.
			storeRelativesChildrenExtendEndtimePs.setLong(1, id.getId());
			storeRelativesChildrenExtendEndtimePs.setTimestamp(2, sqlTimestamp);
			storeRelativesChildrenExtendEndtimePs.executeUpdate();
			storeRelativesChildrenInsertPs.setLong(1, id.getId());
			storeRelativesChildrenInsertPs.setTimestamp(2, sqlTimestamp);
			storeRelativesChildrenInsertPs.executeUpdate();

			// Store the ancestors in the transitive closure table.
			storeRelativesAncestorsExtendEndtimePs.setLong(1, id.getId());
			storeRelativesAncestorsExtendEndtimePs.setTimestamp(2, sqlTimestamp);
			storeRelativesAncestorsExtendEndtimePs.setTimestamp(3, sqlTimestamp);
			storeRelativesAncestorsExtendEndtimePs.setTimestamp(4, sqlTimestamp);
			storeRelativesAncestorsExtendEndtimePs.executeUpdate();
			storeRelativesAncestorsInsertPs.setLong(1, id.getId());
			storeRelativesAncestorsInsertPs.setTimestamp(2, sqlTimestamp);
			storeRelativesAncestorsInsertPs.setTimestamp(3, sqlTimestamp);
			storeRelativesAncestorsInsertPs.setTimestamp(4, sqlTimestamp);
			storeRelativesAncestorsInsertPs.executeUpdate();

			// Store the descendants in the transitive closure table.
			storeRelativesDescendantsExtendEndtimePs.setLong(1, id.getId());
			storeRelativesDescendantsExtendEndtimePs.setTimestamp(2, sqlTimestamp);
			storeRelativesDescendantsExtendEndtimePs.setTimestamp(3, sqlTimestamp);
			storeRelativesDescendantsExtendEndtimePs.setTimestamp(4, sqlTimestamp);
			storeRelativesDescendantsExtendEndtimePs.executeUpdate();
			storeRelativesDescendantsInsertPs.setLong(1, id.getId());
			storeRelativesDescendantsInsertPs.setTimestamp(2, sqlTimestamp);
			storeRelativesDescendantsInsertPs.setTimestamp(3, sqlTimestamp);
			storeRelativesDescendantsInsertPs.setTimestamp(4, sqlTimestamp);
			storeRelativesDescendantsInsertPs.executeUpdate();

			// Store the link between the parents and children in the transitive closure table.
			storeRelativesLinkParentsAndChildrenExtendEndtimePs.setTimestamp(1, sqlTimestamp);
			storeRelativesLinkParentsAndChildrenExtendEndtimePs.executeUpdate();

			storeRelativesLinkParentsAndChildrenInsertPs.setTimestamp(1, sqlTimestamp);
			storeRelativesLinkParentsAndChildrenInsertPs.executeUpdate();

			// Store the link between the ancestors and descendants in the transitive closure table.
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.setTimestamp(1, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.setTimestamp(2, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.setTimestamp(3, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.setTimestamp(4, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.setTimestamp(5, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsExtendEndtimePs.executeUpdate();

			storeRelativesLinkAncestorsAndDescendantsInsertPs.setTimestamp(1, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsInsertPs.setTimestamp(2, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsInsertPs.setTimestamp(3, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsInsertPs.setTimestamp(4, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsInsertPs.setTimestamp(5, sqlTimestamp);
			storeRelativesLinkAncestorsAndDescendantsInsertPs.executeUpdate();

			// Commit all updates
			con.commit();
			// Switch on auto commit.
			con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#inactivateExpressionDefinition(se.liu.imt.mi.
	 * snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public void inactivateExpressionDefinition(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException, ExpressionAlreadyDefined {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		try {
			// Check if the expression id exists in the dbms.
			if (!isExistingExpressionId(id, sqlTimestamp)) {
				throw new NonExistingIdException("The expression id " + id.getId()
						+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
			}

			// Check if an equivalence with a future start time already has been set.
			if (isFutureEquivalentSet(id, sqlTimestamp)) {
				throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
						+ " has already an equivalent id set with a start time in the future.");
			}
			// Check if parent(s) and/or child(ren) with a future start time already has been set.
			if (isFutureRelativeSet(id, sqlTimestamp)) {
				throw new ExpressionAlreadyDefined("The expression with id " + id.getId()
						+ " has already parent(s) and/or child(ren) set with a start time in the future.");
			}

			// Switch of auto commit so all updates are done in the same transaction.
			con.setAutoCommit(false);
			// Inactivate the definition for the expression.
			inactivateExpressionDefinition(id, sqlTimestamp);
			// Commit all updates
			con.commit();
			// Switch on auto commit.
			con.setAutoCommit(true);

		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getExpressionId(java.lang.String,
	 * java.util.Date)
	 */
	@Override
	public ExpressionId getExpressionId(String expression, Date time) throws DataStoreException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		final ExpressionId result;
		try {
			// Look up expression id.
			getExpressionIdPs.setString(1, expression);
			getExpressionIdPs.setTimestamp(2, sqlTimestamp);
			getExpressionIdPs.setTimestamp(3, sqlTimestamp);
			final ResultSet getExpressionIdRs = getExpressionIdPs.executeQuery();
			// Store the result in the variable.
			if (getExpressionIdRs.next()) {
				result = new ExpressionId(getExpressionIdRs.getLong("id"));
			} else {
				result = null;
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getExpression(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public String getExpression(ExpressionId id, Date time) throws DataStoreException, NonExistingIdException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		final String result;
		try {
			// Look up the expression.
			getExpressionPs.setLong(1, id.getId());
			getExpressionPs.setTimestamp(2, sqlTimestamp);
			getExpressionPs.setTimestamp(3, sqlTimestamp);
			final ResultSet getExpressionRs = getExpressionPs.executeQuery();
			// Store the result in the variable.
			if (!getExpressionRs.next()) {
				// The expression id does not exists in the dbms.
				throw new NonExistingIdException(
						"The expression id " + id.getId() + " do not exists in the data store at the specified time.");
			} else {
				result = getExpressionRs.getString("expression");
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getAncestors(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public Set<ExpressionId> getAncestors(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(id, time, getAncestorsPs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getDescendants(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public Set<ExpressionId> getDescendants(ExpressionId id, Date time)
			throws DataStoreException, NonExistingIdException {
		return getRelative(id, time, getDescendantsPs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getParents(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public Set<ExpressionId> getParents(ExpressionId id, Date time) throws DataStoreException, NonExistingIdException {
		return getRelative(id, time, getParentsPs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getChildren(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public Set<ExpressionId> getChildren(ExpressionId id, Date time) throws DataStoreException, NonExistingIdException {
		return getRelative(id, time, getChildrenPs);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getAllExpressions(java.util.Date)
	 */
	@Override
	public Set<Expression> getAllExpressions(Date time) throws DataStoreException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		final HashSet<Expression> result = new HashSet<Expression>();
		try {
			// Look up all expressions.
			final ResultSet getAllExpressionsRs;
			getAllExpressionsPs.setTimestamp(1, sqlTimestamp);
			getAllExpressionsPs.setTimestamp(2, sqlTimestamp);
			getAllExpressionsRs = getAllExpressionsPs.executeQuery();

			// Store the expressions.
			while (getAllExpressionsRs.next()) {
				result.add(new Expression(new ExpressionId(getAllExpressionsRs.getLong("id")),
						getAllExpressionsRs.getString("expression")));
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isExistingId(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, java.util.Date)
	 */
	@Override
	public boolean isExistingId(ExpressionId id, Date time) throws DataStoreException {
		return isExistingId(id, convertOrSetCurrentTimestampToSQLTimestamp(time));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isSubsumingNotEquivalent(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 */
	@Override
	public boolean isSubsumingNotEquivalent(ExpressionId ancestorId, ExpressionId descendantId, Date time)
			throws DataStoreException, NonExistingIdException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);

		// Check if the ids exists in the dbms.
		if (!isExistingId(ancestorId, sqlTimestamp)) {
			throw new NonExistingIdException("The ancestor id " + ancestorId.getId().toString()
					+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
		}
		if (!isExistingId(descendantId, sqlTimestamp)) {
			throw new NonExistingIdException("The descendant id " + descendantId.getId().toString()
					+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
		}

		final boolean result;
		try {
			// Checks if an concept or expression subsumes but is not equivalent to another concept or expression at a
			// specific time.
			isSubsumingNotEquivalentPs.setLong(1, descendantId.getId());
			isSubsumingNotEquivalentPs.setLong(2, ancestorId.getId());
			isSubsumingNotEquivalentPs.setTimestamp(3, sqlTimestamp);
			isSubsumingNotEquivalentPs.setTimestamp(4, sqlTimestamp);
			isSubsumingNotEquivalentPs.setTimestamp(5, sqlTimestamp);
			isSubsumingNotEquivalentPs.setTimestamp(6, sqlTimestamp);
			isSubsumingNotEquivalentPs.setTimestamp(7, sqlTimestamp);
			isSubsumingNotEquivalentPs.setTimestamp(8, sqlTimestamp);
			final ResultSet isRs = isSubsumingNotEquivalentPs.executeQuery();
			isRs.next();
			result = isRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isEquivalent(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 */
	@Override
	public boolean isEquivalent(ExpressionId id1, ExpressionId id2, Date time)
			throws DataStoreException, NonExistingIdException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);

		// Check if the ids exists in the dbms.
		if (!isExistingId(id1, sqlTimestamp)) {
			throw new NonExistingIdException("The id " + id1.getId().toString()
					+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
		}
		if (!isExistingId(id2, sqlTimestamp)) {
			throw new NonExistingIdException("The id " + id2.getId().toString()
					+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
		}

		final boolean result;
		try {
			// Checks if an concept or expression is equivalent to another concept or expression at a specific time.
			isEquivalentPs.setLong(1, id1.getId());
			isEquivalentPs.setLong(2, id2.getId());
			isEquivalentPs.setTimestamp(3, sqlTimestamp);
			isEquivalentPs.setTimestamp(4, sqlTimestamp);
			final ResultSet isEquivalentRs = isEquivalentPs.executeQuery();
			isEquivalentRs.next();
			result = isEquivalentRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#isSubsuming(se.liu.imt.mi.snomedct.
	 * expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,
	 * java.util.Date)
	 */
	@Override
	public boolean isSubsuming(ExpressionId ancestorId, ExpressionId descendantId, Date time)
			throws DataStoreException, NonExistingIdException {
		return isEquivalent(ancestorId, descendantId, time) || isSubsumingNotEquivalent(ancestorId, descendantId, time);
	}

	/**
	 * Check if a specified id exists in the dbms.
	 * 
	 * @param id
	 *            The specified id.
	 * @param sqlTimestamp
	 *            The specific time. A <code>null</code> value is handled as an error.
	 * @return If the id exists in the data store or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isExistingId(final ExpressionId id, final Timestamp sqlTimestamp) throws DataStoreException {
		return isExiId(id, sqlTimestamp, isExistingIdPs);
	}

	/**
	 * Check if a specified expression id exists in the dbms.
	 * 
	 * @param id
	 *            The specified expression id.
	 * @param sqlTimestamp
	 *            The specific time. A <code>null</code> value is handled as an error.
	 * @return If the expression id exists in the date store or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isExistingExpressionId(final ExpressionId id, final Timestamp sqlTimestamp)
			throws DataStoreException {
		return isExiId(id, sqlTimestamp, isExistingExpressionIdPs);
	}

	/**
	 * Check if an id exist as an id for a concept or expression or both depending on the used
	 * <code>PreparedStatement</code>.
	 * 
	 * @param id
	 *            The id to check the existence for.
	 * @param sqlTimestamp
	 *            The specific time. A <code>null</code> value is handled as an error.
	 * @param isPs
	 *            The <code>PreparedStatement</code> to use .
	 * @return If the id exist or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isExiId(final ExpressionId id, final Timestamp sqlTimestamp, final PreparedStatement isPs)
			throws DataStoreException {
		final boolean result;
		try {
			final ResultSet isExistingIdRs;
			isPs.setLong(1, id.getId());
			isPs.setTimestamp(2, sqlTimestamp);
			isPs.setTimestamp(3, sqlTimestamp);
			isExistingIdRs = isPs.executeQuery();
			isExistingIdRs.next();
			result = isExistingIdRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Check if an equivalence with a future start already has been set for the expression.
	 * 
	 * @param id
	 *            The expression's id to check for if an equivalence with a future start already has been set.
	 * @param sqlTimestamp
	 *            The specific time to do the check. A <code>null</code> value is handled as an error.
	 * @return If the expression already has an equivalence set in the future or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isFutureEquivalentSet(final ExpressionId id, final Timestamp sqlTimestamp)
			throws DataStoreException {
		final boolean result;
		try {
			isFutureEquivalentSetPs.setLong(1, id.getId());
			isFutureEquivalentSetPs.setTimestamp(2, sqlTimestamp);
			ResultSet isFutureRelativeSetRs = isFutureEquivalentSetPs.executeQuery();
			isFutureRelativeSetRs.next();
			result = isFutureRelativeSetRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Check if parent(s) and/or child(ren) with a future start already has been set for the expression.
	 * 
	 * @param id
	 *            The expression's id to check for if parent(s) and/or child(ren) with a future start already has been
	 *            set.
	 * @param sqlTimestamp
	 *            The specific time to do the check. A <code>null</code> value is handled as an error.
	 * @return If the expression already has parent(s) and/or child(ren) set in the future or not.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private boolean isFutureRelativeSet(ExpressionId id, final Timestamp sqlTimestamp) throws DataStoreException {
		final boolean result;
		try {
			isFutureRelativeSetPs.setLong(1, id.getId());
			isFutureRelativeSetPs.setLong(2, id.getId());
			isFutureRelativeSetPs.setTimestamp(3, sqlTimestamp);
			ResultSet isFutureRelativeSetRs = isFutureRelativeSetPs.executeQuery();
			isFutureRelativeSetRs.next();
			result = isFutureRelativeSetRs.getBoolean("exist");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Inactivate an expression's definition from the data store.
	 * 
	 * @param id
	 *            The expression's id to inactivate the definition for.
	 * @param time
	 *            The time the expression's definition was inactivated.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 */
	private void inactivateExpressionDefinition(final ExpressionId id, final Timestamp sqlTimestamp)
			throws DataStoreException {
		try {
			// Create new relationships for the concepts in an equivalence group if the current relationships for the
			// concepts with the relationships in the group is going to be inactivated.
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setTimestamp(1, sqlTimestamp);
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setLong(2, id.getId());
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setTimestamp(3, sqlTimestamp);
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setTimestamp(4, sqlTimestamp);
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setTimestamp(5, sqlTimestamp);
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.setTimestamp(6, sqlTimestamp);
			inactivateRelativesCreateNewRelationshipsForEquivalencePs.executeUpdate();

			// Create new direct relationships between the parent(s) and child(ren) of the concept which is going to be
			// retired.
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(1, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setLong(2, id.getId());
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(3, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(4, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(5, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(6, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setLong(7, id.getId());
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(8, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.setTimestamp(9, sqlTimestamp);
			inactivateRelativesCreateNewDirectRelationshipsPs.executeUpdate();

			// Set the end time to the relationships that is going to be retired.
			inactivateRelativesSetEndtimePs.setTimestamp(1, sqlTimestamp);
			inactivateRelativesSetEndtimePs.setLong(2, id.getId());
			inactivateRelativesSetEndtimePs.setLong(3, id.getId());
			inactivateRelativesSetEndtimePs.setTimestamp(4, sqlTimestamp);
			inactivateRelativesSetEndtimePs.setTimestamp(5, sqlTimestamp);
			inactivateRelativesSetEndtimePs.executeUpdate();

			// Delete relationships with the same starttime as the time the retirement is done.
			inactivateRelativesDeleteWithCurrentStartTimePs.setTimestamp(1, sqlTimestamp);
			inactivateRelativesDeleteWithCurrentStartTimePs.setLong(2, id.getId());
			inactivateRelativesDeleteWithCurrentStartTimePs.setLong(3, id.getId());
			inactivateRelativesDeleteWithCurrentStartTimePs.executeUpdate();

			// Set the end time to the second last expression equivalence in the group.
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(1, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(2, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(3, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setLong(4, id.getId());
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(5, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(6, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setLong(7, id.getId());
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(8, sqlTimestamp);
			inactivateEquivalenceGroupSetEndtimePs.setTimestamp(9, sqlTimestamp);
			inactivateRelativesDeleteWithCurrentStartTimePs.executeUpdate();

			// Delete the second last expression equivalence in the group.
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setTimestamp(1, sqlTimestamp);
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setLong(2, id.getId());
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setTimestamp(3, sqlTimestamp);
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setTimestamp(4, sqlTimestamp);
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setLong(5, id.getId());
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setTimestamp(6, sqlTimestamp);
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.setTimestamp(7, sqlTimestamp);
			inactivateEquivalenceGroupDeleteWithCurrentStartTimePs.executeUpdate();

			// Set the end time to the expression equivalence that is going to be retired.
			inactivateEquivalenceSetEndtimePs.setTimestamp(1, sqlTimestamp);
			inactivateEquivalenceSetEndtimePs.setLong(2, id.getId());
			inactivateEquivalenceSetEndtimePs.setTimestamp(3, sqlTimestamp);
			inactivateEquivalenceSetEndtimePs.setTimestamp(4, sqlTimestamp);
			inactivateEquivalenceSetEndtimePs.executeUpdate();

			// Delete expression equivalence with the same starttime as the time the retirement is done.
			inactivateEquivalenceDeleteWithCurrentStartTimePs.setTimestamp(1, sqlTimestamp);
			inactivateEquivalenceDeleteWithCurrentStartTimePs.setLong(2, id.getId());
			inactivateEquivalenceDeleteWithCurrentStartTimePs.executeUpdate();
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/**
	 * 
	 * Look up relatives at a specific time from the data store using a <code>PreparedStatement</code>.
	 * 
	 * @param id
	 *            The expression id to look up the relatives to.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @param getRelativePs
	 *            The <code>PreparedStatement</code> to use for finding the relatives.
	 * @return The relatives' expression ids.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NonExistingIdException
	 *             Thrown if the expression id does not exist in the data store.
	 */
	private HashSet<ExpressionId> getRelative(ExpressionId id, Date time, PreparedStatement getRelativePs)
			throws DataStoreException, NonExistingIdException {
		final Timestamp sqlTimestamp = convertOrSetCurrentTimestampToSQLTimestamp(time);
		final HashSet<ExpressionId> result = new HashSet<ExpressionId>();
		try {
			// Check if the id exists in the dbms.
			if (!isExistingId(id, sqlTimestamp)) {
				throw new NonExistingIdException("The id " + id.getId().toString()
						+ " do not exists in the data store at the time " + sqlTimestamp.toString() + ".");
			}
			final ResultSet getRelativeRs;
			// Look up the relatives in the dbms.
			getRelativePs.setLong(1, id.getId());
			getRelativePs.setTimestamp(2, sqlTimestamp);
			getRelativePs.setTimestamp(3, sqlTimestamp);
			getRelativePs.setTimestamp(4, sqlTimestamp);
			getRelativePs.setTimestamp(5, sqlTimestamp);
			getRelativePs.setTimestamp(6, sqlTimestamp);
			getRelativePs.setTimestamp(7, sqlTimestamp);
			getRelativeRs = getRelativePs.executeQuery();
			// Store the result.
			while (getRelativeRs.next()) {
				result.add(new ExpressionId(getRelativeRs.getLong("id")));
			}
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
		return result;
	}

	/**
	 * Convert a <code>Date</code> or take the current time and insert it into a SQL <code>Timestamp</code>.
	 * <p>
	 * If the <code>time</code> parameter is not <code>null</code>, then the method returns the <code>time</code>
	 * parameter as a SQL <code>Timestamp</code>.
	 * <p>
	 * If the <code>time</code> parameter is <code>null</code>, then the method returns the current time as a SQL
	 * <code>Timestamp</code>.
	 * 
	 * @param time
	 *            The <code>time</code> to convert or <code><null</code> for the current time.
	 * @return The resulting time as a SQL <code>Timestamp</code>.
	 */
	private Timestamp convertOrSetCurrentTimestampToSQLTimestamp(final Date time) {
		return (time != null ? new Timestamp(time.getTime()) : new Timestamp(java.lang.System.currentTimeMillis()));
	}
}