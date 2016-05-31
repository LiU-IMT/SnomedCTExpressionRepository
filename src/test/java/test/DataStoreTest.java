/**
 * 
 */
package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyDefined;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyExistsException;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStoreService;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * 
 * JUnit test for class {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore}
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class DataStoreTest {

	/**
	 * A <code>String</code> with the url to the database.
	 */
	private static String url = null;
	/**
	 * A <code>String</code> with the user name to the database.
	 */
	private static String username = null;
	/**
	 * A <code>String</code> with the password to the database.
	 */
	private static String password = null;

	/**
	 * A <code>Connection</code> to use for preparation before and clean up after the tests.
	 */
	private static Connection con = null;
	/**
	 * A <code>Statement</code> to use for preparation before and clean up after the tests.
	 */
	private static Statement stmt = null;

	/**
	 * The <code>DataStoreService</code> used for preparation before and clean up after the tests.
	 */
	private static DataStoreService dss = null;

	/**
	 * The <code>DataStore</code> class to test.
	 */
	private DataStore ds;

	/**
	 * The test's start time.
	 */
	private static Date startTime = null;;

	/**
	 * @throws java.lang.Exception
	 *             If it is any problems with the database.
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Store the timestamp the execution started.
		startTime = new Date();

		// Fetch the database connection details.
		Configuration config = null;
		config = new XMLConfiguration("config.xml");
		url = config.getString("database.url");
		username = config.getString("database.username");
		password = config.getString("database.password");

		// Set up a database connection and a service data store for the test methods to use for preparation before and
		// clean up after the tests.
		Class.forName("org.postgresql.Driver");
		con = DriverManager.getConnection(url, username, password);
		stmt = con.createStatement();
		dss = new DataStoreService(url, username, password);
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is any problems with the database.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		try {
			dss.finalize();
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is any problems with the database.
	 */
	@Before
	public void setUp() throws Exception {
		// Create a new data store class.
		ds = new DataStore(url, username, password);
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is any problems with the database.
	 */
	@After
	public void tearDown() throws Exception {
		try {
			ds.finalize();
		} catch (Throwable e) {
			throw new Exception(e);
		}

		// Clean up the database.
		dss.restoreDataStore(startTime);
		// dss.restoreDataStore(new GregorianCalendar(2016, 1, 1, 0, 0, 0).getTime());
	}

	/**
	 * Test method for {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#DataStore()} .
	 */
	@Test
	public final void testDataStore() {
		assertNotNull("No DataStore object created", ds);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpression(java.lang.String, java.util.Date)}
	 * .
	 */
	@Test
	public final void testStoreExpression() {
		final String expressionWithoutDate = "10";
		final ExpressionId expressionIdWithoutDate;
		final ExpressionId expressionIdWithoutDateTested;

		try {
			expressionIdWithoutDate = ds.storeExpression(expressionWithoutDate, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		assertNotNull("No ExpressionId returned after expression insertation without date.", expressionIdWithoutDate);
		assertTrue(
				"The returned ExpressionId after expression insertation without date, "
						+ expressionIdWithoutDate.getId().toString() + ", is invalid.",
				expressionIdWithoutDate.getId() < 0);

		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT id FROM expressions WHERE expression = '"
					+ expressionWithoutDate + "' AND endtime = 'infinity'::timestamp;");

			assertTrue("No stored id for the expression " + expressionWithoutDate + " in the database.", rs1.next());
			expressionIdWithoutDateTested = new ExpressionId(rs1.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The stored id for the expression " + expressionWithoutDate
						+ " without date is incorrect in the database.",
				expressionIdWithoutDate, expressionIdWithoutDateTested);

		try {
			ds.storeExpression(expressionWithoutDate, null);
			fail("A ExpressionAlreadyExistsException should be thrown when inserting multiple versions of the same expression.");
		} catch (ExpressionAlreadyExistsException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		// --------------------------------------------------------------------------------

		final String expressionWithDate = "20";
		final ExpressionId expressionIdWithDate;
		final ExpressionId expressionIdWithDateTested;
		final Date insertTime = new GregorianCalendar(2110, 12, 03, 16, 14, 32).getTime();

		try {
			expressionIdWithDate = ds.storeExpression(expressionWithDate, insertTime);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		assertNotNull("No ExpressionId returned after expression insertation with date.", expressionIdWithDate);
		assertTrue("The returned ExpressionId after expression insertation with date, "
				+ expressionIdWithDate.getId().toString() + ", is invalid.", expressionIdWithDate.getId() < 0);

		try {
			final ResultSet rs2 = stmt
					.executeQuery("SELECT id FROM expressions WHERE expression = '" + expressionWithDate
							+ "' AND starttime = '" + toSQL(insertTime) + "' AND endtime = 'infinity'::timestamp;");

			assertTrue("No stored id for the expression " + expressionWithDate + " at the time " + insertTime
					+ " in the database.", rs2.next());
			expressionIdWithDateTested = new ExpressionId(rs2.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored id for the expression " + expressionWithDate + " at the time " + insertTime
				+ " is incorrect in the database.", expressionIdWithDate, expressionIdWithDateTested);

		try {
			ds.storeExpression(expressionWithDate, insertTime);
			fail("A ExpressionAlreadyExistsException should be thrown when inserting multiple expressions.");
		} catch (ExpressionAlreadyExistsException e1) {
			// Everything is correct.
		} catch (DataStoreException e1) {
			throw new AssertionError(e1);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpressionEquivalence(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testStoreExpressionEquivalence() {
		final String expression1 = "31";
		final String expression2 = "32";
		final String expression3 = "33";
		final String expression4 = "34";
		final Date insertTime1 = new GregorianCalendar(2107, 2, 1, 9, 12, 33).getTime();
		final Date insertTime2 = new GregorianCalendar(2111, 8, 14, 7, 1, 45).getTime();
		final Date insertTime3 = new GregorianCalendar(2112, 11, 18, 2, 26, 4).getTime();
		final ExpressionId expression1Id;
		final ExpressionId expression2Id;
		final ExpressionId expression3Id;
		final ExpressionId expression4Id;
		final ExpressionId conceptIdKingdomAnimalia = new ExpressionId((long) 387961004);
		final ExpressionId conceptIdBloodBloodBankProcedure = new ExpressionId((long) 59524001);
		final ExpressionId conceptIdBloodCompatibilityTest = new ExpressionId((long) 250404007);

		try {
			expression1Id = ds.storeExpression(expression1, null);
			expression2Id = ds.storeExpression(expression2, null);
			expression3Id = ds.storeExpression(expression3, null);
			expression4Id = ds.storeExpression(expression4, insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression1Id);
			equivalentExpressionIds.add(conceptIdKingdomAnimalia);
			testDifferentExpressionEquivalences(expression1Id, conceptIdKingdomAnimalia, null, equivalentExpressionIds);

		}

		try {
			ds.storeExpressionEquivalence(expression1Id, expression1Id, null);
			fail("A NonExistingIdException should be thrown when trying to explicitly store that an expression is equivalent to itself.");
		} catch (NonExistingIdException e) {
			// Everything is correct
		} catch (DataStoreException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression1Id);
			equivalentExpressionIds.add(expression2Id);
			testDifferentExpressionEquivalences(expression1Id, expression2Id, null, equivalentExpressionIds);
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression1Id);
			equivalentExpressionIds.add(expression3Id);
			testDifferentExpressionEquivalences(expression1Id, expression3Id, null, equivalentExpressionIds);
		}

		try {
			ds.storeExpressionEquivalence(expression1Id, expression4Id, null);
			fail("A NonExistingIdException should be thrown when trying to set an expression equivalence to an expression with a starttime in the future.");
		} catch (NonExistingIdException e) {
			// Everything is correct
		} catch (DataStoreException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> parent = new HashSet<ExpressionId>();
			parent.add(conceptIdBloodBloodBankProcedure);
			final Set<ExpressionId> child = new HashSet<ExpressionId>();
			child.add(conceptIdBloodCompatibilityTest);
			try {
				ds.storeExpressionParentsAndChildren(expression1Id, parent, child, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression1Id);
			equivalentExpressionIds.add(expression3Id);
			testDifferentExpressionEquivalences(expression1Id, expression3Id, null, equivalentExpressionIds);
		}

		// --------------------------------------------------------------------------------

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression1Id);
			equivalentExpressionIds.add(expression3Id);
			equivalentExpressionIds.add(expression4Id);
			testDifferentExpressionEquivalences(expression4Id, expression1Id, insertTime2, equivalentExpressionIds);
		}

		try {
			ds.storeExpressionEquivalence(expression3Id, conceptIdKingdomAnimalia, insertTime2);
			ds.storeExpressionEquivalence(expression3Id, expression1Id, insertTime1);
			fail("A ExpressionAlreadyDefined should be thrown when trying to set an expression equivalence to an earlier starttime than an already set expression equivalence.");
		} catch (ExpressionAlreadyDefined e) {
			// Everything is correct
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(expression3Id);
			equivalentExpressionIds.add(expression2Id);
			testDifferentExpressionEquivalences(expression3Id, expression2Id, insertTime3, equivalentExpressionIds);
		}

		{
			final Set<ExpressionId> equivalentExpressionIds = new HashSet<ExpressionId>();
			equivalentExpressionIds.add(conceptIdKingdomAnimalia);
			equivalentExpressionIds.add(expression4Id);
			testDifferentExpressionEquivalences(expression4Id, conceptIdKingdomAnimalia, insertTime3,
					equivalentExpressionIds);
		}

		try {
			final ResultSet rs1 = stmt
					.executeQuery("SELECT Count(*) AS numberof FROM equivalents WHERE id = '" + expression4Id
							+ "' AND starttime = '" + toSQL(insertTime2) + "' AND endtime = '" + toSQL(insertTime3)
							+ "' AND " + "equivalentid IN (SELECT equivalentid FROM  equivalents WHERE id = "
							+ expression1Id + " AND starttime <= '" + toSQL(insertTime2) + "');");
			rs1.next();
			final long numberOfRows = rs1.getLong(1);
			assertTrue("The equivalence for the expression with id " + expression3Id + " and the expression with id "
					+ expression1Id + " created at the starttime " + toSQL(insertTime2)
					+ " should have been inactivated at the endtime " + toSQL(insertTime3) + " in the data store.",
					numberOfRows == 1);
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Store and test if a specific expression equivalence is properly stored in the data store.
	 * 
	 * @param id
	 *            The expression's id to store the equivalence for.
	 * @param equivalentExpressionId
	 *            The expression's or concept's id for the equivalent expression.
	 * @param time
	 *            The time the expression equivalence is stored. A <code>null</code> value is handled as the current
	 *            time.
	 * @param equivalentExpressionIds
	 *            The expressions or concepts id for the equivalent expressions to compare with.
	 * @throws AssertionError
	 *             If something goes wrong with the tests.
	 */
	private void testDifferentExpressionEquivalences(final ExpressionId id, final ExpressionId equivalentExpressionId,
			final Date time, final Set<ExpressionId> equivalentExpressionIds) throws AssertionError {
		try {
			ds.storeExpressionEquivalence(id, equivalentExpressionId, time);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}
		final Set<ExpressionId> equivalenceTested = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT id FROM equivalents WHERE "
					+ (time != null ? "starttime <= '" + toSQL(time) + "' AND " : "")
					+ "endtime = 'infinity'::timestamp " + "AND equivalentid IN "
					+ "(SELECT equivalentid FROM  equivalents WHERE "
					+ (time != null ? "starttime <= '" + toSQL(time) + "' AND " : "")
					+ "endtime = 'infinity'::timestamp " + "AND id = " + id.getId().toString() + " );");
			while (rs1.next()) {
				equivalenceTested.add(new ExpressionId(rs1.getLong("id")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored equivalence for the expression with id " + id
				+ (time != null ? " created at the starttime " + toSQL(time) + " " : "") + " was not the expected.",
				equivalentExpressionIds, equivalenceTested);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpressionParentsAndChildren(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Set, java.util.Set, java.util.Date)}
	 * .
	 */
	@Test
	public final void testStoreExpressionParentsAndChildren() {
		final ExpressionId conceptIdMechanicalForce = new ExpressionId((long) 285653008);
		final String expressionBrotherToMechanicalForce = "40";
		final ExpressionId expressionIdTestExpression;
		try {
			expressionIdTestExpression = ds.storeExpression(expressionBrotherToMechanicalForce, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdMechanicalForce, null);

		final ExpressionId conceptIdAdministrativeProcedure = new ExpressionId((long) 14734007);
		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdAdministrativeProcedure, null);
		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdMechanicalForce, null);

		try {
			ds.storeExpressionEquivalence(expressionIdTestExpression, conceptIdAdministrativeProcedure, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdMechanicalForce, null);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2107, 4, 3, 2, 14, 54).getTime();
		final Date insertTime2 = new GregorianCalendar(2110, 2, 6, 11, 34, 22).getTime();
		final Date insertTime3 = new GregorianCalendar(2113, 11, 15, 18, 23, 42).getTime();

		final ExpressionId conceptIdElectricField = new ExpressionId((long) 32646006);
		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdElectricField, insertTime2);

		try {
			ds.storeExpressionParentsAndChildren(expressionIdTestExpression, null, null, insertTime1);
			fail("A ExpressionAlreadyDefined should be thrown when trying to set an expression's parents and children to an earlier starttime than already set parents and children.");
		} catch (ExpressionAlreadyDefined e) {
			// Everything is correct
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		testDifferentParentsAndChildren(expressionIdTestExpression, conceptIdMechanicalForce, insertTime3);
	}

	/**
	 * Store and test if specific expression relatives are properly stored in the data store.
	 * 
	 * @param idToUpdateRelativesFor
	 *            The concept of expression id to update the relatives for.
	 * @param idSupplierOfRelatives
	 *            The concept of expression id used as the supplier of the relatives.
	 * @param time
	 *            The time the expression's parent(s) and child(ren) were stored. A <code>null</code> value is handled
	 *            as the current time.
	 * @throws AssertionError
	 *             If something goes wrong with the tests.
	 */
	private void testDifferentParentsAndChildren(final ExpressionId idToUpdateRelativesFor,
			final ExpressionId idSupplierOfRelatives, final Date time) throws AssertionError {
		final Set<ExpressionId> suppliedParents = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT destinationid FROM transitiveclosure WHERE sourceid = "
					+ idSupplierOfRelatives.getId() + " AND directrelation = true AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				suppliedParents.add(new ExpressionId(rs1.getLong("destinationid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> suppliedChildren = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT sourceid FROM transitiveclosure WHERE destinationid = "
					+ idSupplierOfRelatives.getId() + " AND directrelation = true AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				suppliedChildren.add(new ExpressionId(rs1.getLong("sourceid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionParentsAndChildren(idToUpdateRelativesFor, suppliedParents, suppliedChildren, time);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> testedParents = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT destinationid FROM transitiveclosure WHERE sourceid = "
					+ idToUpdateRelativesFor.getId() + " AND directrelation = true AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				testedParents.add(new ExpressionId(rs1.getLong("destinationid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and the real parents to the expression with id " + idToUpdateRelativesFor.getId()
				+ " do not match. (The concept or expression with id " + idSupplierOfRelatives
				+ " was used as a supplier of the relatives.)", suppliedParents, testedParents);

		final Set<ExpressionId> testedChildren = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT sourceid FROM transitiveclosure WHERE destinationid = "
					+ idToUpdateRelativesFor.getId() + " AND directrelation = true AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				testedChildren.add(new ExpressionId(rs1.getLong("sourceid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and the real children to the expression with id " + idToUpdateRelativesFor.getId()
				+ " do not match. (The concept or expression with id " + idSupplierOfRelatives
				+ " was used as a supplier of the relatives.)", suppliedChildren, testedChildren);

		final Set<ExpressionId> suppliedAncestors = new HashSet<ExpressionId>();
		final Set<ExpressionId> testedAncestors = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT destinationid FROM transitiveclosure WHERE sourceid = "
					+ idSupplierOfRelatives.getId() + " AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				suppliedAncestors.add(new ExpressionId(rs1.getLong("destinationid")));
			}
			final ResultSet rs2 = stmt.executeQuery("SELECT destinationid FROM transitiveclosure WHERE sourceid = "
					+ idToUpdateRelativesFor.getId() + " AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs2.next()) {
				testedAncestors.add(new ExpressionId(rs2.getLong("destinationid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and the real ancestors to the expression with id " + idToUpdateRelativesFor.getId()
				+ " do not match. (The concept or expression with id " + idSupplierOfRelatives
				+ " was used as a supplier of the relatives.)", suppliedAncestors, testedAncestors);

		final Set<ExpressionId> suppliedDescendants = new HashSet<ExpressionId>();
		final Set<ExpressionId> testedDescendants = new HashSet<ExpressionId>();
		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT sourceid FROM transitiveclosure WHERE destinationid = "
					+ idSupplierOfRelatives.getId() + " AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs1.next()) {
				suppliedDescendants.add(new ExpressionId(rs1.getLong("sourceid")));
			}
			final ResultSet rs2 = stmt.executeQuery("SELECT sourceid FROM transitiveclosure WHERE  destinationid = "
					+ idToUpdateRelativesFor.getId() + " AND starttime <= "
					+ (time != null ? "'" + toSQL(time) + "' " : "'now()' ") + "AND endtime = 'infinity'::timestamp ;");
			while (rs2.next()) {
				testedDescendants.add(new ExpressionId(rs2.getLong("sourceid")));
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and the real ancestors to the expression with id " + idToUpdateRelativesFor.getId()
				+ " do not match. (The concept or expression with id " + idSupplierOfRelatives
				+ " was used as a supplier of the relatives.)", suppliedDescendants, testedDescendants);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#inactivateExpressionDefinition(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testInactivateExpressionDefinition() {
		final String expressionTestExpression = "50";
		final ExpressionId expressionIdTestExpression;
		try {
			expressionIdTestExpression = ds.storeExpression(expressionTestExpression, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		final ExpressionId conceptIdPaperDevice = new ExpressionId((long) 50833004);
		final ExpressionId conceptIdCarbonPaperDevice = new ExpressionId((long) 25864001);
		final ExpressionId conceptIdCorrectingPaperDevice = new ExpressionId((long) 21735008);
		final ExpressionId conceptIdPhotocopyPaperDevice = new ExpressionId((long) 85935002);
		final ExpressionId conceptIdPhotographyPaperDevice = new ExpressionId((long) 55263001);
		final ExpressionId conceptIdWritingPaperDevice = new ExpressionId((long) 55287004);

		testInactivatedDefinitions(expressionIdTestExpression, null);

		try {
			ds.storeExpressionEquivalence(expressionIdTestExpression, conceptIdPaperDevice, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdTestExpression, conceptIdCarbonPaperDevice, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		try {
			ds.inactivateExpressionDefinition(expressionIdTestExpression, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		testInactivatedDefinitions(expressionIdTestExpression, null);

		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdPaperDevice);

			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdCorrectingPaperDevice);
			children.add(conceptIdPhotocopyPaperDevice);
			children.add(conceptIdPhotographyPaperDevice);
			children.add(conceptIdWritingPaperDevice);

			try {
				ds.storeExpressionParentsAndChildren(expressionIdTestExpression, parents, children, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}

			try {
				ds.inactivateExpressionDefinition(expressionIdTestExpression, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}

			testInactivatedDefinitions(expressionIdTestExpression, null);
		}

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2109, 4, 1, 2, 45, 03).getTime();
		final Date insertTime2 = new GregorianCalendar(2115, 2, 23, 1, 34, 38).getTime();
		final Date insertTime3 = new GregorianCalendar(2119, 10, 12, 4, 2, 42).getTime();
		final Date insertTime4 = new GregorianCalendar(2120, 3, 22, 6, 20, 11).getTime();
		final Date insertTime5 = new GregorianCalendar(2122, 9, 24, 7, 21, 33).getTime();

		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdCorrectingPaperDevice);

			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdPhotocopyPaperDevice);
			children.add(conceptIdPhotographyPaperDevice);

			try {
				ds.storeExpressionParentsAndChildren(expressionIdTestExpression, parents, children, insertTime2);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}

			try {
				ds.inactivateExpressionDefinition(expressionIdTestExpression, insertTime1);
				fail("A ExpressionAlreadyDefined should be thrown when trying to inactivate an expression's definition before the expression has been defined.");
			} catch (ExpressionAlreadyDefined e) {
				// Everything is correct
			} catch (DataStoreException | NonExistingIdException e) {
				throw new AssertionError(e);
			}

			try {
				ds.inactivateExpressionDefinition(expressionIdTestExpression, insertTime2);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}

			testInactivatedDefinitions(expressionIdTestExpression, insertTime2);

		}

		try {
			ds.storeExpressionEquivalence(expressionIdTestExpression, conceptIdCorrectingPaperDevice, insertTime3);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		try {
			ds.inactivateExpressionDefinition(expressionIdTestExpression, insertTime2);
			fail("A ExpressionAlreadyDefined should be thrown when trying to inactivate an expression's definition before the expression has been defined.");
		} catch (ExpressionAlreadyDefined e) {
			// Everything is correct
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		try {
			ds.inactivateExpressionDefinition(expressionIdTestExpression, insertTime5);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		testInactivatedDefinitions(expressionIdTestExpression, insertTime5);

		try {
			final ResultSet rs1 = stmt.executeQuery("SELECT Count(*) AS numberof FROM equivalents WHERE id = "
					+ expressionIdTestExpression.getId().toString() + " AND starttime <= '" + toSQL(insertTime4)
					+ "' AND '" + toSQL(insertTime4) + "' < endtime " + ";");
			rs1.next();
			final long numberOfRows = rs1.getLong(1);
			assertTrue("The expression with id " + expressionIdTestExpression.getId().toString()
					+ " has no entries in the equivalents table at the time " + insertTime4.toString()
					+ ", but one entry was expected.", numberOfRows > 0);
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test if a specific expression has got its definitions inactivated.
	 * 
	 * @param id
	 *            The expression to check the inactivation for.
	 * @param time
	 *            The time to check the inactivation.
	 * @throws AssertionError
	 *             If something goes wrong with the tests.
	 */
	private void testInactivatedDefinitions(final ExpressionId id, Date time) throws AssertionError {
		try {
			final ResultSet rs1 = stmt
					.executeQuery(
							"SELECT Count(*) AS numberof FROM equivalents WHERE id = " + id.getId().toString() + " AND "
									+ (time != null
											? "starttime <= '" + toSQL(time) + "' AND '" + toSQL(time) + "' < endtime "
											: "endtime = 'infinity'::timestamp ")
									+ ";");
			rs1.next();
			final long numberOfRows = rs1.getLong(1);
			assertTrue(
					"The expression with id " + id.getId().toString() + " has " + numberOfRows
							+ " current entries in the equivalents table, but these shouldn't be any current entries in that table.",
					numberOfRows == 0);
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet rs1 = stmt
					.executeQuery(
							"SELECT Count(*) AS numberof FROM transitiveclosure WHERE (sourceid = "
									+ id.getId().toString() + " OR destinationid = " + id.getId().toString() + ") AND "
									+ (time != null
											? "starttime <= '" + toSQL(time) + "' AND '" + toSQL(time) + "' < endtime "
											: "endtime = 'infinity'::timestamp ")
									+ ";");
			rs1.next();
			final long numberOfRows = rs1.getLong(1);
			assertTrue(
					"The expression with id " + id.getId().toString() + " has " + numberOfRows
							+ " current entries in the transitiveclosure table, but these shouldn't be any current entries in that table.",
					numberOfRows == 0);
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore##getExpressionId(java.lang.String,
	 * java.util.Date} .
	 */
	@Test
	public final void testGetExpressionId() {
		final String expressionWithoutTime = "60";
		final ExpressionId expressionIdWithoutTime;
		final ExpressionId expressionIdWithoutTimeTested;

		try {
			expressionIdWithoutTime = ds.storeExpression(expressionWithoutTime, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionIdWithoutTimeTested = ds.getExpressionId(expressionWithoutTime, null);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		assertEquals("The stored and retrieved expression ids for the expression " + expressionWithoutTime
				+ " do not match.", expressionIdWithoutTime, expressionIdWithoutTimeTested);

		// --------------------------------------------------------------------------------

		final String expressionWithTime = "61";
		final ExpressionId expressionIdWithTime;
		final ExpressionId expressionIdWithTimeTested1;
		final ExpressionId expressionIdWithTimeTested2;
		final ExpressionId expressionIdWithTimeTested3;
		final Date insertTime1 = new GregorianCalendar(2108, 02, 04, 12, 34, 82).getTime();
		final Date insertTime2 = new GregorianCalendar(2110, 12, 03, 16, 14, 32).getTime();
		final Date insertTime3 = new GregorianCalendar(2111, 03, 06, 02, 34, 54).getTime();

		try {
			expressionIdWithTime = ds.storeExpression(expressionWithTime, insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionIdWithTimeTested1 = ds.getExpressionId(expressionWithTime, insertTime1);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertNull(
				"The expression " + expressionWithTime + " was stored at the time " + insertTime2.toString()
						+ ", but already at the time " + insertTime1.toString()
						+ " the data store returns that the expression has an expression id.",
				expressionIdWithTimeTested1);

		try {
			expressionIdWithTimeTested2 = ds.getExpressionId(expressionWithTime, insertTime2);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and retrieved expression ids for the expression " + expressionWithTime
				+ " at the time " + insertTime2.toString() + " do not match.", expressionIdWithTime,
				expressionIdWithTimeTested2);

		try {
			expressionIdWithTimeTested3 = ds.getExpressionId(expressionWithTime, insertTime3);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and retrieved expression ids for the expression " + expressionWithTime
				+ " at the time " + insertTime3.toString() + " do not match.", expressionIdWithTime,
				expressionIdWithTimeTested3);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getExpression(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetExpression() {
		final String expressionWithoutTime = "70";
		final ExpressionId expressionIdWithoutTime;
		final String expressionWithoutTimeTested;

		try {
			expressionIdWithoutTime = ds.storeExpression(expressionWithoutTime, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithoutTimeTested = ds.getExpression(expressionIdWithoutTime, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals("The stored and retrieved expression for the expression with id " + expressionIdWithoutTime
				+ " do not match.", expressionWithoutTime, expressionWithoutTimeTested);

		// --------------------------------------------------------------------------------

		final String expressionWithTime = "71";
		final ExpressionId expressionIdWithTime;
		final String expressionWithTimeTested1;
		final String expressionWithTimeTested2;
		final String expressionWithTimeTested3;
		final Date insertTime1 = new GregorianCalendar(2107, 03, 12, 02, 56, 21).getTime();
		final Date insertTime2 = new GregorianCalendar(2109, 03, 24, 07, 16, 12).getTime();
		final Date insertTime3 = new GregorianCalendar(2113, 04, 10, 12, 44, 24).getTime();

		try {
			expressionIdWithTime = ds.storeExpression(expressionWithTime, insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithTimeTested1 = ds.getExpression(expressionIdWithTime, insertTime1);
			fail("A NonExistingIdException should be thrown when asking for a expression at a time before the expression "
					+ expressionWithTimeTested1 + " was insterded.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithTimeTested2 = ds.getExpression(expressionIdWithTime, insertTime2);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The stored and retrieved expression for the expression with id " + expressionIdWithTime
						+ " at the time " + insertTime2.toString() + " do not match.",
				expressionWithTime, expressionWithTimeTested2);

		try {
			expressionWithTimeTested3 = ds.getExpression(expressionIdWithTime, insertTime3);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The stored and retrieved expression for the expression with id " + expressionIdWithTime
						+ " at the time " + insertTime3.toString() + " do not match.",
				expressionWithTime, expressionWithTimeTested3);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getAncestors(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetAncestors() {
		final ExpressionId conceptIdSnomedCtConcept = new ExpressionId((long) 138875005);
		final ExpressionId conceptIdPhysicalForce = new ExpressionId((long) 78621006);
		final ExpressionId conceptIdAltitude = new ExpressionId((long) 76661004);

		final Set<ExpressionId> conceptAncestors = new HashSet<ExpressionId>();
		conceptAncestors.add(conceptIdSnomedCtConcept);
		conceptAncestors.add(conceptIdPhysicalForce);
		final Set<ExpressionId> conceptAncestorsTested;

		try {
			conceptAncestorsTested = ds.getAncestors(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString()
				+ " were not the expected.", conceptAncestors, conceptAncestorsTested);

		// --------------------------------------------------------------------------------

		final ExpressionId expressionIdEqualToSnomedCtConcept;
		final ExpressionId expressionIdEqualToPhysicalForce;
		final ExpressionId expressionIdParentToPhysicalForce;
		final ExpressionId expressionIdEqualToAltitude;
		final ExpressionId expressionIdParentToAltitude;

		try {
			expressionIdEqualToSnomedCtConcept = ds.storeExpression("80", null);
			expressionIdEqualToPhysicalForce = ds.storeExpression("81", null);
			expressionIdParentToPhysicalForce = ds.storeExpression("82", null);
			expressionIdEqualToAltitude = ds.storeExpression("83", null);
			expressionIdParentToAltitude = ds.storeExpression("84", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToSnomedCtConcept, conceptIdSnomedCtConcept, null);

			ds.storeExpressionEquivalence(expressionIdEqualToPhysicalForce, conceptIdPhysicalForce, null);
			final Set<ExpressionId> conceptPhysicalForceSet = new HashSet<ExpressionId>();
			conceptPhysicalForceSet.add(conceptIdPhysicalForce);
			ds.storeExpressionParentsAndChildren(expressionIdParentToPhysicalForce, null, conceptPhysicalForceSet,
					null);

			ds.storeExpressionEquivalence(expressionIdEqualToAltitude, conceptIdAltitude, null);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdParentToAltitude, null, conceptAltitudeSet, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionAncestors = new HashSet<ExpressionId>();
		expressionAncestors.addAll(conceptAncestors);
		expressionAncestors.add(expressionIdEqualToSnomedCtConcept);
		expressionAncestors.add(expressionIdEqualToPhysicalForce);
		expressionAncestors.add(expressionIdParentToPhysicalForce);
		expressionAncestors.add(expressionIdParentToAltitude);
		final Set<ExpressionId> expressionAncestorsTested;

		try {
			expressionAncestorsTested = ds.getAncestors(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString()
						+ " after addition of expression were not the expected.",
				expressionAncestors, expressionAncestorsTested);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2102, 01, 02, 15, 32, 11).getTime();
		final Date insertTime2 = new GregorianCalendar(2104, 05, 22, 19, 33, 45).getTime();
		final Date insertTime3 = new GregorianCalendar(2108, 11, 30, 16, 30, 42).getTime();
		final Date insertTime4 = new GregorianCalendar(2110, 02, 21, 12, 23, 51).getTime();

		final ExpressionId expressionIdEqualToSnomedCtConceptInsertTime2;
		final ExpressionId expressionIdEqualToPhysicalForceInsertTime2;
		final ExpressionId expressionIdParentToPhysicalForceInsertTime2;
		final ExpressionId expressionIdEqualToAltitudeInsertTime2;
		final ExpressionId expressionIdParentToAltitudeInsertTime2;

		try {
			expressionIdEqualToSnomedCtConceptInsertTime2 = ds.storeExpression("080", insertTime2);
			expressionIdEqualToPhysicalForceInsertTime2 = ds.storeExpression("081", insertTime2);
			expressionIdParentToPhysicalForceInsertTime2 = ds.storeExpression("082", insertTime2);
			expressionIdEqualToAltitudeInsertTime2 = ds.storeExpression("083", insertTime2);
			expressionIdParentToAltitudeInsertTime2 = ds.storeExpression("084", insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToSnomedCtConceptInsertTime2, conceptIdSnomedCtConcept,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToPhysicalForceInsertTime2, conceptIdPhysicalForce,
					insertTime3);
			final Set<ExpressionId> conceptPhysicalForceInsertTime2Set = new HashSet<ExpressionId>();
			conceptPhysicalForceInsertTime2Set.add(conceptIdPhysicalForce);
			ds.storeExpressionParentsAndChildren(expressionIdParentToPhysicalForceInsertTime2, null,
					conceptPhysicalForceInsertTime2Set, insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToAltitudeInsertTime2, conceptIdAltitude, insertTime3);
			final Set<ExpressionId> conceptAltitudeInsertTime2Set = new HashSet<ExpressionId>();
			conceptAltitudeInsertTime2Set.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdParentToAltitudeInsertTime2, null,
					conceptAltitudeInsertTime2Set, insertTime3);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionAncestorsInsertTime3 = new HashSet<ExpressionId>();
		expressionAncestorsInsertTime3.addAll(expressionAncestors);
		expressionAncestorsInsertTime3.add(expressionIdEqualToSnomedCtConceptInsertTime2);
		expressionAncestorsInsertTime3.add(expressionIdEqualToPhysicalForceInsertTime2);
		expressionAncestorsInsertTime3.add(expressionIdParentToPhysicalForceInsertTime2);
		expressionAncestorsInsertTime3.add(expressionIdParentToAltitudeInsertTime2);
		final Set<ExpressionId> expressionAncestorsTestedInsertTime1;
		final Set<ExpressionId> expressionAncestorsTestedInsertTime2;
		final Set<ExpressionId> expressionAncestorsTestedInsertTime3;
		final Set<ExpressionId> expressionAncestorsTestedInsertTime4;

		try {
			expressionAncestorsTestedInsertTime1 = ds.getAncestors(conceptIdAltitude, insertTime1);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime1.toString() + " were not the expected.",
				expressionAncestors, expressionAncestorsTestedInsertTime1);

		try {
			expressionAncestorsTestedInsertTime2 = ds.getAncestors(conceptIdAltitude, insertTime2);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime2.toString() + " were not the expected.",
				expressionAncestors, expressionAncestorsTestedInsertTime2);

		try {
			expressionAncestorsTestedInsertTime3 = ds.getAncestors(conceptIdAltitude, insertTime3);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime3.toString() + " were not the expected.",
				expressionAncestorsInsertTime3, expressionAncestorsTestedInsertTime3);

		try {
			expressionAncestorsTestedInsertTime4 = ds.getAncestors(conceptIdAltitude, insertTime4);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved ancestors to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime4.toString() + " were not the expected.",
				expressionAncestorsInsertTime3, expressionAncestorsTestedInsertTime4);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getDescendants(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetDescendants() {
		final ExpressionId conceptIdAltitude = new ExpressionId((long) 76661004);
		final ExpressionId conceptIdHighAltitude = new ExpressionId((long) 87588000);
		final ExpressionId conceptIdLowAltitude = new ExpressionId((long) 10035008);
		final ExpressionId conceptIdBelowSeaLevel = new ExpressionId((long) 72110006);

		final Set<ExpressionId> conceptDescendants = new HashSet<ExpressionId>();
		conceptDescendants.add(conceptIdHighAltitude);
		conceptDescendants.add(conceptIdLowAltitude);
		conceptDescendants.add(conceptIdBelowSeaLevel);
		final Set<ExpressionId> conceptDescendantsTested;

		try {
			conceptDescendantsTested = ds.getDescendants(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		assertEquals("The retrieved descendants to the concept " + conceptIdAltitude.getId().toString()
				+ " were not the expected.", conceptDescendants, conceptDescendantsTested);

		// --------------------------------------------------------------------------------

		final ExpressionId expressionIdEqualToAltitude;
		final ExpressionId expressionIdChildToAltitude;
		final ExpressionId expressionIdEqualToHighAltitude;
		final ExpressionId expressionIdChildToHighAltitude;
		final ExpressionId expressionIdEqualToLowAltitude;
		final ExpressionId expressionIdChildToLowAltitude;
		final ExpressionId expressionIdEqualToBelowSeaLevel;
		final ExpressionId expressionIdChildToBelowSeaLevel;

		try {
			expressionIdEqualToAltitude = ds.storeExpression("90", null);
			expressionIdChildToAltitude = ds.storeExpression("91", null);
			expressionIdEqualToHighAltitude = ds.storeExpression("92", null);
			expressionIdChildToHighAltitude = ds.storeExpression("93", null);
			expressionIdEqualToLowAltitude = ds.storeExpression("94", null);
			expressionIdChildToLowAltitude = ds.storeExpression("95", null);
			expressionIdEqualToBelowSeaLevel = ds.storeExpression("96", null);
			expressionIdChildToBelowSeaLevel = ds.storeExpression("97", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToAltitude, conceptIdAltitude, null);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToAltitude, conceptAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToHighAltitude, conceptIdHighAltitude, null);
			final Set<ExpressionId> conceptHighAltitudeSet = new HashSet<ExpressionId>();
			conceptHighAltitudeSet.add(conceptIdHighAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToHighAltitude, conceptHighAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToLowAltitude, conceptIdLowAltitude, null);
			final Set<ExpressionId> conceptLowAltitudeSet = new HashSet<ExpressionId>();
			conceptLowAltitudeSet.add(conceptIdLowAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToLowAltitude, conceptLowAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToBelowSeaLevel, conceptIdBelowSeaLevel, null);
			final Set<ExpressionId> conceptBelowSeaLevelSet = new HashSet<ExpressionId>();
			conceptBelowSeaLevelSet.add(conceptIdBelowSeaLevel);
			ds.storeExpressionParentsAndChildren(expressionIdChildToBelowSeaLevel, conceptBelowSeaLevelSet, null, null);

		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionDescendants = new HashSet<ExpressionId>();
		expressionDescendants.addAll(conceptDescendants);
		expressionDescendants.add(expressionIdChildToAltitude);
		expressionDescendants.add(expressionIdEqualToHighAltitude);
		expressionDescendants.add(expressionIdChildToHighAltitude);
		expressionDescendants.add(expressionIdEqualToLowAltitude);
		expressionDescendants.add(expressionIdChildToLowAltitude);
		expressionDescendants.add(expressionIdEqualToBelowSeaLevel);
		expressionDescendants.add(expressionIdChildToBelowSeaLevel);
		final Set<ExpressionId> expressionDescendantsTested;

		try {
			expressionDescendantsTested = ds.getDescendants(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved descendants to the concept " + conceptIdAltitude.getId().toString()
						+ " after addition of expression were not the expected.",
				expressionDescendants, expressionDescendantsTested);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2115, 03, 01, 21, 11, 34).getTime();
		final Date insertTime2 = new GregorianCalendar(2116, 02, 28, 12, 12, 32).getTime();
		final Date insertTime3 = new GregorianCalendar(2118, 10, 21, 1, 33, 44).getTime();
		final Date insertTime4 = new GregorianCalendar(2119, 04, 11, 15, 35, 56).getTime();

		final ExpressionId expressionIdEqualToAltitudeInsertTime2;
		final ExpressionId expressionIdChildToAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToHighAltitudeInsertTime2;
		final ExpressionId expressionIdChildToHighAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToLowAltitudeInsertTime2;
		final ExpressionId expressionIdChildToLowAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToBelowSeaLevelInsertTime2;
		final ExpressionId expressionIdChildToBelowSeaLevelInsertTime2;

		try {
			expressionIdEqualToAltitudeInsertTime2 = ds.storeExpression("090", null);
			expressionIdChildToAltitudeInsertTime2 = ds.storeExpression("091", null);
			expressionIdEqualToHighAltitudeInsertTime2 = ds.storeExpression("092", null);
			expressionIdChildToHighAltitudeInsertTime2 = ds.storeExpression("093", null);
			expressionIdEqualToLowAltitudeInsertTime2 = ds.storeExpression("094", null);
			expressionIdChildToLowAltitudeInsertTime2 = ds.storeExpression("095", null);
			expressionIdEqualToBelowSeaLevelInsertTime2 = ds.storeExpression("096", null);
			expressionIdChildToBelowSeaLevelInsertTime2 = ds.storeExpression("097", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToAltitudeInsertTime2, conceptIdAltitude, insertTime3);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToAltitudeInsertTime2, conceptAltitudeSet, null,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToHighAltitudeInsertTime2, conceptIdHighAltitude,
					insertTime3);
			final Set<ExpressionId> conceptHighAltitudeSet = new HashSet<ExpressionId>();
			conceptHighAltitudeSet.add(conceptIdHighAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToHighAltitudeInsertTime2, conceptHighAltitudeSet,
					null, insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToLowAltitudeInsertTime2, conceptIdLowAltitude, insertTime3);
			final Set<ExpressionId> conceptLowAltitudeSet = new HashSet<ExpressionId>();
			conceptLowAltitudeSet.add(conceptIdLowAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToLowAltitudeInsertTime2, conceptLowAltitudeSet, null,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToBelowSeaLevelInsertTime2, conceptIdBelowSeaLevel,
					insertTime3);
			final Set<ExpressionId> conceptBelowSeaLevelSet = new HashSet<ExpressionId>();
			conceptBelowSeaLevelSet.add(conceptIdBelowSeaLevel);
			ds.storeExpressionParentsAndChildren(expressionIdChildToBelowSeaLevelInsertTime2, conceptBelowSeaLevelSet,
					null, insertTime3);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionDescendantsInsertTime3 = new HashSet<ExpressionId>();
		expressionDescendantsInsertTime3.addAll(expressionDescendants);
		expressionDescendantsInsertTime3.add(expressionIdChildToAltitudeInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdEqualToHighAltitudeInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdChildToHighAltitudeInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdEqualToLowAltitudeInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdChildToLowAltitudeInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdEqualToBelowSeaLevelInsertTime2);
		expressionDescendantsInsertTime3.add(expressionIdChildToBelowSeaLevelInsertTime2);
		final Set<ExpressionId> expressionDescendantsTestedInsertTime1;
		final Set<ExpressionId> expressionDescendantsTestedInsertTime2;
		final Set<ExpressionId> expressionDescendantsTestedInsertTime3;
		final Set<ExpressionId> expressionDescendantsTestedInsertTime4;

		try {
			expressionDescendantsTestedInsertTime1 = ds.getDescendants(conceptIdAltitude, insertTime1);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved descendants to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime1.toString() + " were not the expected.",
				expressionDescendants, expressionDescendantsTestedInsertTime1);

		try {
			expressionDescendantsTestedInsertTime2 = ds.getDescendants(conceptIdAltitude, insertTime2);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved descendants to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime2.toString() + " were not the expected.",
				expressionDescendants, expressionDescendantsTestedInsertTime2);

		try {
			expressionDescendantsTestedInsertTime3 = ds.getDescendants(conceptIdAltitude, insertTime3);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved descendants to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime3.toString() + " were not the expected.",
				expressionDescendantsInsertTime3, expressionDescendantsTestedInsertTime3);

		try {
			expressionDescendantsTestedInsertTime4 = ds.getDescendants(conceptIdAltitude, insertTime4);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved descendants to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime4.toString() + " were not the expected.",
				expressionDescendantsInsertTime3, expressionDescendantsTestedInsertTime4);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getParents(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetParents() {
		final ExpressionId conceptIdSnomedCtConcept = new ExpressionId((long) 138875005);
		final ExpressionId conceptIdPhysicalForce = new ExpressionId((long) 78621006);
		final ExpressionId conceptIdAltitude = new ExpressionId((long) 76661004);

		final Set<ExpressionId> conceptParents = new HashSet<ExpressionId>();
		conceptParents.add(conceptIdPhysicalForce);
		final Set<ExpressionId> conceptParentsTested;

		try {
			conceptParentsTested = ds.getParents(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved parent(s) to the concept " + conceptIdAltitude.getId().toString()
				+ " were not the expected.", conceptParents, conceptParentsTested);

		// --------------------------------------------------------------------------------

		final ExpressionId expressionIdEqualToSnomedCtConcept;
		final ExpressionId expressionIdEqualToPhysicalForce;
		final ExpressionId expressionIdParentToPhysicalForce;
		final ExpressionId expressionIdEqualToAltitude;
		final ExpressionId expressionIdParentToAltitude;

		try {
			expressionIdEqualToSnomedCtConcept = ds.storeExpression("100", null);
			expressionIdEqualToPhysicalForce = ds.storeExpression("101", null);
			expressionIdParentToPhysicalForce = ds.storeExpression("102", null);
			expressionIdEqualToAltitude = ds.storeExpression("103", null);
			expressionIdParentToAltitude = ds.storeExpression("104", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToSnomedCtConcept, conceptIdSnomedCtConcept, null);

			ds.storeExpressionEquivalence(expressionIdEqualToPhysicalForce, conceptIdPhysicalForce, null);
			final Set<ExpressionId> conceptPhysicalForceSet = new HashSet<ExpressionId>();
			conceptPhysicalForceSet.add(conceptIdPhysicalForce);
			ds.storeExpressionParentsAndChildren(expressionIdParentToPhysicalForce, null, conceptPhysicalForceSet,
					null);

			ds.storeExpressionEquivalence(expressionIdEqualToAltitude, conceptIdAltitude, null);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdParentToAltitude, null, conceptAltitudeSet, null);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionParents = new HashSet<ExpressionId>();
		expressionParents.addAll(conceptParents);
		expressionParents.add(expressionIdEqualToPhysicalForce);
		expressionParents.add(expressionIdParentToAltitude);
		final Set<ExpressionId> expressionParentsTested;

		try {
			expressionParentsTested = ds.getParents(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved parents to the concept " + conceptIdAltitude.getId().toString()
						+ " after addition of expression were not the expected.",
				expressionParents, expressionParentsTested);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2105, 03, 12, 18, 22, 13).getTime();
		final Date insertTime2 = new GregorianCalendar(2108, 03, 15, 8, 12, 4).getTime();
		final Date insertTime3 = new GregorianCalendar(2111, 2, 3, 12, 21, 2).getTime();
		final Date insertTime4 = new GregorianCalendar(2115, 4, 11, 1, 23, 58).getTime();

		final ExpressionId expressionIdEqualToSnomedCtConceptInsertTime2;
		final ExpressionId expressionIdEqualToPhysicalForceInsertTime2;
		final ExpressionId expressionIdParentToPhysicalForceInsertTime2;
		final ExpressionId expressionIdEqualToAltitudeInsertTime2;
		final ExpressionId expressionIdParentToAltitudeInsertTime2;

		try {
			expressionIdEqualToSnomedCtConceptInsertTime2 = ds.storeExpression("0100", insertTime2);
			expressionIdEqualToPhysicalForceInsertTime2 = ds.storeExpression("0101", insertTime2);
			expressionIdParentToPhysicalForceInsertTime2 = ds.storeExpression("0102", insertTime2);
			expressionIdEqualToAltitudeInsertTime2 = ds.storeExpression("0103", insertTime2);
			expressionIdParentToAltitudeInsertTime2 = ds.storeExpression("0104", insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToSnomedCtConceptInsertTime2, conceptIdSnomedCtConcept,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToPhysicalForceInsertTime2, conceptIdPhysicalForce,
					insertTime3);
			final Set<ExpressionId> conceptPhysicalForceInsertTime2Set = new HashSet<ExpressionId>();
			conceptPhysicalForceInsertTime2Set.add(conceptIdPhysicalForce);
			ds.storeExpressionParentsAndChildren(expressionIdParentToPhysicalForceInsertTime2, null,
					conceptPhysicalForceInsertTime2Set, insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToAltitudeInsertTime2, conceptIdAltitude, insertTime3);
			final Set<ExpressionId> conceptAltitudeInsertTime2Set = new HashSet<ExpressionId>();
			conceptAltitudeInsertTime2Set.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdParentToAltitudeInsertTime2, null,
					conceptAltitudeInsertTime2Set, insertTime3);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionParentsInsertTime3 = new HashSet<ExpressionId>();
		expressionParentsInsertTime3.addAll(expressionParents);
		expressionParentsInsertTime3.add(expressionIdEqualToPhysicalForceInsertTime2);
		expressionParentsInsertTime3.add(expressionIdParentToAltitudeInsertTime2);
		final Set<ExpressionId> expressionParentsTestedInsertTime1;
		final Set<ExpressionId> expressionParentsTestedInsertTime2;
		final Set<ExpressionId> expressionParentsTestedInsertTime3;
		final Set<ExpressionId> expressionParentsTestedInsertTime4;

		try {
			expressionParentsTestedInsertTime1 = ds.getParents(conceptIdAltitude, insertTime1);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved parents to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime1.toString() + " were not the expected.",
				expressionParents, expressionParentsTestedInsertTime1);

		try {
			expressionParentsTestedInsertTime2 = ds.getParents(conceptIdAltitude, insertTime2);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved parents to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime2.toString() + " were not the expected.",
				expressionParents, expressionParentsTestedInsertTime2);

		try {
			expressionParentsTestedInsertTime3 = ds.getParents(conceptIdAltitude, insertTime3);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved parents to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime3.toString() + " were not the expected.",
				expressionParentsInsertTime3, expressionParentsTestedInsertTime3);

		try {
			expressionParentsTestedInsertTime4 = ds.getParents(conceptIdAltitude, insertTime4);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved parents to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime4.toString() + " were not the expected.",
				expressionParentsInsertTime3, expressionParentsTestedInsertTime4);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getChildren(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetChildren() {
		final ExpressionId conceptIdAltitude = new ExpressionId((long) 76661004);
		final ExpressionId conceptIdHighAltitude = new ExpressionId((long) 87588000);
		final ExpressionId conceptIdLowAltitude = new ExpressionId((long) 10035008);
		final ExpressionId conceptIdBelowSeaLevel = new ExpressionId((long) 72110006);

		final Set<ExpressionId> conceptChildren = new HashSet<ExpressionId>();
		conceptChildren.add(conceptIdHighAltitude);
		conceptChildren.add(conceptIdLowAltitude);
		final Set<ExpressionId> conceptChildrenTested;

		try {
			conceptChildrenTested = ds.getChildren(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		assertEquals("The retrieved children to the concept " + conceptIdAltitude.getId().toString()
				+ " were not the expected.", conceptChildren, conceptChildrenTested);

		// --------------------------------------------------------------------------------

		final ExpressionId expressionIdEqualToAltitude;
		final ExpressionId expressionIdChildToAltitude;
		final ExpressionId expressionIdEqualToHighAltitude;
		final ExpressionId expressionIdChildToHighAltitude;
		final ExpressionId expressionIdEqualToLowAltitude;
		final ExpressionId expressionIdChildToLowAltitude;
		final ExpressionId expressionIdEqualToBelowSeaLevel;
		final ExpressionId expressionIdChildToBelowSeaLevel;

		try {
			expressionIdEqualToAltitude = ds.storeExpression("110", null);
			expressionIdChildToAltitude = ds.storeExpression("111", null);
			expressionIdEqualToHighAltitude = ds.storeExpression("112", null);
			expressionIdChildToHighAltitude = ds.storeExpression("113", null);
			expressionIdEqualToLowAltitude = ds.storeExpression("114", null);
			expressionIdChildToLowAltitude = ds.storeExpression("115", null);
			expressionIdEqualToBelowSeaLevel = ds.storeExpression("116", null);
			expressionIdChildToBelowSeaLevel = ds.storeExpression("117", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToAltitude, conceptIdAltitude, null);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToAltitude, conceptAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToHighAltitude, conceptIdHighAltitude, null);
			final Set<ExpressionId> conceptHighAltitudeSet = new HashSet<ExpressionId>();
			conceptHighAltitudeSet.add(conceptIdHighAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToHighAltitude, conceptHighAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToLowAltitude, conceptIdLowAltitude, null);
			final Set<ExpressionId> conceptLowAltitudeSet = new HashSet<ExpressionId>();
			conceptLowAltitudeSet.add(conceptIdLowAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToLowAltitude, conceptLowAltitudeSet, null, null);

			ds.storeExpressionEquivalence(expressionIdEqualToBelowSeaLevel, conceptIdBelowSeaLevel, null);
			final Set<ExpressionId> conceptBelowSeaLevelSet = new HashSet<ExpressionId>();
			conceptBelowSeaLevelSet.add(conceptIdBelowSeaLevel);
			ds.storeExpressionParentsAndChildren(expressionIdChildToBelowSeaLevel, conceptBelowSeaLevelSet, null, null);

		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionChildren = new HashSet<ExpressionId>();
		expressionChildren.addAll(conceptChildren);
		expressionChildren.add(expressionIdChildToAltitude);
		expressionChildren.add(expressionIdEqualToHighAltitude);
		expressionChildren.add(expressionIdEqualToLowAltitude);
		final Set<ExpressionId> expressionChildrenTested;

		try {
			expressionChildrenTested = ds.getChildren(conceptIdAltitude, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved children to the concept " + conceptIdAltitude.getId().toString()
						+ " after addition of expression were not the expected.",
				expressionChildren, expressionChildrenTested);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2101, 04, 03, 10, 15, 33).getTime();
		final Date insertTime2 = new GregorianCalendar(2103, 01, 22, 13, 15, 54).getTime();
		final Date insertTime3 = new GregorianCalendar(2105, 01, 11, 13, 32, 53).getTime();
		final Date insertTime4 = new GregorianCalendar(2107, 06, 14, 11, 33, 26).getTime();

		final ExpressionId expressionIdEqualToAltitudeInsertTime2;
		final ExpressionId expressionIdChildToAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToHighAltitudeInsertTime2;
		final ExpressionId expressionIdChildToHighAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToLowAltitudeInsertTime2;
		final ExpressionId expressionIdChildToLowAltitudeInsertTime2;
		final ExpressionId expressionIdEqualToBelowSeaLevelInsertTime2;
		final ExpressionId expressionIdChildToBelowSeaLevelInsertTime2;

		try {
			expressionIdEqualToAltitudeInsertTime2 = ds.storeExpression("0110", null);
			expressionIdChildToAltitudeInsertTime2 = ds.storeExpression("0111", null);
			expressionIdEqualToHighAltitudeInsertTime2 = ds.storeExpression("0112", null);
			expressionIdChildToHighAltitudeInsertTime2 = ds.storeExpression("0113", null);
			expressionIdEqualToLowAltitudeInsertTime2 = ds.storeExpression("0114", null);
			expressionIdChildToLowAltitudeInsertTime2 = ds.storeExpression("0115", null);
			expressionIdEqualToBelowSeaLevelInsertTime2 = ds.storeExpression("0116", null);
			expressionIdChildToBelowSeaLevelInsertTime2 = ds.storeExpression("0117", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expressionIdEqualToAltitudeInsertTime2, conceptIdAltitude, insertTime3);
			final Set<ExpressionId> conceptAltitudeSet = new HashSet<ExpressionId>();
			conceptAltitudeSet.add(conceptIdAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToAltitudeInsertTime2, conceptAltitudeSet, null,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToHighAltitudeInsertTime2, conceptIdHighAltitude,
					insertTime3);
			final Set<ExpressionId> conceptHighAltitudeSet = new HashSet<ExpressionId>();
			conceptHighAltitudeSet.add(conceptIdHighAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToHighAltitudeInsertTime2, conceptHighAltitudeSet,
					null, insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToLowAltitudeInsertTime2, conceptIdLowAltitude, insertTime3);
			final Set<ExpressionId> conceptLowAltitudeSet = new HashSet<ExpressionId>();
			conceptLowAltitudeSet.add(conceptIdLowAltitude);
			ds.storeExpressionParentsAndChildren(expressionIdChildToLowAltitudeInsertTime2, conceptLowAltitudeSet, null,
					insertTime3);

			ds.storeExpressionEquivalence(expressionIdEqualToBelowSeaLevelInsertTime2, conceptIdBelowSeaLevel,
					insertTime3);
			final Set<ExpressionId> conceptBelowSeaLevelSet = new HashSet<ExpressionId>();
			conceptBelowSeaLevelSet.add(conceptIdBelowSeaLevel);
			ds.storeExpressionParentsAndChildren(expressionIdChildToBelowSeaLevelInsertTime2, conceptBelowSeaLevelSet,
					null, insertTime3);
		} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		final Set<ExpressionId> expressionChildrenInsertTime3 = new HashSet<ExpressionId>();
		expressionChildrenInsertTime3.addAll(expressionChildren);
		expressionChildrenInsertTime3.add(expressionIdChildToAltitudeInsertTime2);
		expressionChildrenInsertTime3.add(expressionIdEqualToHighAltitudeInsertTime2);
		expressionChildrenInsertTime3.add(expressionIdEqualToLowAltitudeInsertTime2);
		final Set<ExpressionId> expressionChildrenTestedInsertTime1;
		final Set<ExpressionId> expressionChildrenTestedInsertTime2;
		final Set<ExpressionId> expressionChildrenTestedInsertTime3;
		final Set<ExpressionId> expressionChildrenTestedInsertTime4;

		try {
			expressionChildrenTestedInsertTime1 = ds.getChildren(conceptIdAltitude, insertTime1);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved children to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime1.toString() + " were not the expected.",
				expressionChildren, expressionChildrenTestedInsertTime1);

		try {
			expressionChildrenTestedInsertTime2 = ds.getChildren(conceptIdAltitude, insertTime2);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved children to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime2.toString() + " were not the expected.",
				expressionChildren, expressionChildrenTestedInsertTime2);

		try {
			expressionChildrenTestedInsertTime3 = ds.getChildren(conceptIdAltitude, insertTime3);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved children to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime3.toString() + " were not the expected.",
				expressionChildrenInsertTime3, expressionChildrenTestedInsertTime3);

		try {
			expressionChildrenTestedInsertTime4 = ds.getChildren(conceptIdAltitude, insertTime4);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertEquals(
				"The retrieved children to the concept " + conceptIdAltitude.getId().toString() + " at the time "
						+ insertTime4.toString() + " were not the expected.",
				expressionChildrenInsertTime3, expressionChildrenTestedInsertTime4);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore#getAllExpressions(java.util.Date)} .
	 */
	@Test
	public final void testGetAllExpressions() {
		final String expressionString0 = "120";
		final String expressionString1 = "121";
		final ExpressionId expressionId0;
		final ExpressionId expressionId1;

		try {
			expressionId0 = ds.storeExpression(expressionString0, null);
			expressionId1 = ds.storeExpression(expressionString1, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		final Expression expression0 = new Expression(expressionId0, expressionString0);
		final Expression expression1 = new Expression(expressionId1, expressionString1);

		final Set<Expression> expressions = new HashSet<Expression>();
		expressions.add(expression0);
		expressions.add(expression1);
		final Set<Expression> expressionsTested;

		try {
			expressionsTested = ds.getAllExpressions(null);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved expressions were not the expected.", expressions, expressionsTested);

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2102, 02, 21, 11, 13, 14).getTime();
		final Date insertTime2 = new GregorianCalendar(2105, 03, 31, 9, 16, 33).getTime();
		final Date insertTime3 = new GregorianCalendar(2106, 07, 28, 04, 21, 30).getTime();

		final String expressionString2 = "122";
		final String expressionString3 = "123";
		final ExpressionId expressionId2;
		final ExpressionId expressionId3;

		try {
			expressionId2 = ds.storeExpression(expressionString2, insertTime2);
			expressionId3 = ds.storeExpression(expressionString3, insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		final Expression expression2 = new Expression(expressionId2, expressionString2);
		final Expression expression3 = new Expression(expressionId3, expressionString3);

		final Set<Expression> expressionsInsertTime2 = new HashSet<Expression>();
		expressionsInsertTime2.addAll(expressions);
		expressionsInsertTime2.add(expression2);
		expressionsInsertTime2.add(expression3);
		final Set<Expression> expressionsTestedInsertTime1;
		final Set<Expression> expressionsTestedInsertTime2;
		final Set<Expression> expressionsTestedInsertTime3;

		try {
			expressionsTestedInsertTime1 = ds.getAllExpressions(insertTime1);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved expressions at the time " + insertTime1.toString() + " were not the expected.",
				expressions, expressionsTestedInsertTime1);

		try {
			expressionsTestedInsertTime2 = ds.getAllExpressions(insertTime2);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved expressions at the time " + insertTime2.toString() + " were not the expected.",
				expressionsInsertTime2, expressionsTestedInsertTime2);

		try {
			expressionsTestedInsertTime3 = ds.getAllExpressions(insertTime3);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertEquals("The retrieved expressions at the time " + insertTime3.toString() + " were not the expected.",
				expressionsInsertTime2, expressionsTestedInsertTime3);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isExistingId(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsExistingId() {
		final ExpressionId conceptIdInvalid = new ExpressionId((long) 1000000550);
		try {
			assertFalse(
					"The method states that the invalid concept id 1000000550 does exist in the data store at the current time.",
					ds.isExistingId(conceptIdInvalid, null));
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		// --------------------------------------------------------------------------------

		final Date insertTime1 = new GregorianCalendar(2000, 01, 11, 05, 23, 38).getTime();
		final Date insertTime2 = new GregorianCalendar(2002, 04, 30, 10, 15, 33).getTime();
		final ExpressionId conceptIdBodyStructure = new ExpressionId((long) 123037004);
		try {
			assertTrue(
					"The method states that the concept 123037004 | Body structure | doesn't exist in the data store at the current time, but that is not correct.",
					ds.isExistingId(conceptIdBodyStructure, null));
			assertFalse(
					"The method states that the concept 123037004 | Body structure | does exist in the data store at the time "
							+ insertTime1
							+ ", but that is not correct because that is before the first release of SNOMED CT.",
					ds.isExistingId(conceptIdBodyStructure, insertTime1));
			assertTrue(
					"The method states that the concept 123037004 | Body structure | doesn't exist in the data store at the time "
							+ insertTime2 + ", but that is not correct.",
					ds.isExistingId(conceptIdBodyStructure, insertTime2));
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		// --------------------------------------------------------------------------------

		final Date insertTime4 = new GregorianCalendar(2103, 02, 31, 11, 12, 35).getTime();
		final Date insertTime5 = new GregorianCalendar(2106, 01, 22, 03, 13, 11).getTime();
		final Date insertTime6 = new GregorianCalendar(2109, 05, 02, 04, 23, 26).getTime();
		final ExpressionId expressionId;
		try {
			expressionId = ds.storeExpression("130", insertTime5);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		try {
			assertFalse(
					"The method states that the expression id " + expressionId.getId().toString()
							+ " does exist in the data store at the current time, but that is not correct.",
					ds.isExistingId(expressionId, null));
			assertFalse(
					"The method states that the expression id " + expressionId.getId().toString()
							+ " does exist in the data store at the time " + insertTime4 + ", but that is not correct.",
					ds.isExistingId(expressionId, insertTime4));
			assertTrue("The method states that the expression id " + expressionId.getId().toString()
					+ " doesn't exist in the data store at the time " + insertTime5 + ", but that is not correct.",
					ds.isExistingId(expressionId, insertTime5));
			assertTrue("The method states that the expression id " + expressionId.getId().toString()
					+ " doesn't exist in the data store at the time " + insertTime5 + ", but that is not correct.",
					ds.isExistingId(expressionId, insertTime6));
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isSubsumingNotEquivalent(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsSubsumingNotEquivalent() {
		final ExpressionId conceptIdBodyStructure = new ExpressionId((long) 123037004);
		final ExpressionId conceptIdProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptIdProcedureByMethod = new ExpressionId((long) 128927009);
		final Date insertTime1 = new GregorianCalendar(2000, 03, 13, 02, 12, 58).getTime();
		final Date insertTime2 = new GregorianCalendar(2003, 01, 21, 11, 55, 44).getTime();

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, conceptIdProcedure,
				null);

		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, null);

		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdBodyStructure, conceptIdProcedureByMethod, null);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedure, null);

		caseAncestorIdIsInvalidTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod,
				insertTime1);
		caseDescendantIdIsInvalidTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod,
				insertTime1);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod,
				insertTime2);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, conceptIdProcedure,
				insertTime2);

		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, insertTime2);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, insertTime2);

		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdBodyStructure, conceptIdProcedureByMethod, insertTime2);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, conceptIdProcedure, insertTime2);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdOrganism = new ExpressionId((long) 410607006);
		final ExpressionId conceptIdQualifierValue = new ExpressionId((long) 362981000);
		final ExpressionId conceptIdAction = new ExpressionId((long) 129264002);

		final ExpressionId expressionIdEqualToOrganism;
		final ExpressionId expressionIdEqualToQualifierValue;
		final ExpressionId expressionIdEqualToAction;

		try {
			expressionIdEqualToOrganism = ds.storeExpression("140", null);
			ds.storeExpressionEquivalence(expressionIdEqualToOrganism, conceptIdOrganism, null);
			expressionIdEqualToQualifierValue = ds.storeExpression("141", null);
			ds.storeExpressionEquivalence(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
			expressionIdEqualToAction = ds.storeExpression("142", null);
			ds.storeExpressionEquivalence(expressionIdEqualToAction, conceptIdAction, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue, conceptIdAction,
				null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdAction,
				expressionIdEqualToQualifierValue, null);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdQualifierValue, expressionIdEqualToAction,
				null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToAction,
				conceptIdQualifierValue, null);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue,
				expressionIdEqualToAction, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToAction,
				expressionIdEqualToQualifierValue, null);

		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue, conceptIdAction, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdAction, expressionIdEqualToQualifierValue, null);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToAction, conceptIdQualifierValue, null);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToAction, expressionIdEqualToQualifierValue,
				null);

		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToOrganism, conceptIdAction, null);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdQualifierValue, expressionIdEqualToQualifierValue, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToQualifierValue, expressionIdEqualToQualifierValue,
				null);

		// --------------------------------------------------------------------------------

		final Date insertTime3 = new GregorianCalendar(2105, 01, 11, 21, 16, 23).getTime();
		final Date insertTime4 = new GregorianCalendar(2107, 03, 12, 11, 31, 41).getTime();
		final Date insertTime5 = new GregorianCalendar(2111, 03, 01, 06, 13, 06).getTime();

		final ExpressionId expressionIdEqualToBodyStructure;
		final ExpressionId expressionIdEqualToProcedure;
		final ExpressionId expressionIdEqualToProcedureByMethod;

		try {
			expressionIdEqualToBodyStructure = ds.storeExpression("143", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToBodyStructure, conceptIdBodyStructure, insertTime4);
			expressionIdEqualToProcedure = ds.storeExpression("144", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
			expressionIdEqualToProcedureByMethod = ds.storeExpression("145", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedureByMethod, conceptIdProcedureByMethod,
					insertTime4);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseAncestorIdIsInvalidTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime3);
		caseDescendantIdIsInvalidTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime3);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod,
				expressionIdEqualToProcedure, insertTime4);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				conceptIdProcedure, insertTime4);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure,
				expressionIdEqualToProcedureByMethod, insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime4);

		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime4);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime4);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime4);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime4);

		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod,
				insertTime4);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedure, insertTime4);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToBodyStructure,
				expressionIdEqualToProcedureByMethod, insertTime4);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedure,
				insertTime4);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod,
				expressionIdEqualToProcedure, insertTime5);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				conceptIdProcedure, insertTime5);
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure,
				expressionIdEqualToProcedureByMethod, insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime5);

		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime5);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime5);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime5);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime5);

		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod,
				insertTime5);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProcedure, expressionIdEqualToProcedure, insertTime5);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, conceptIdProcedure, insertTime5);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdEqualToBodyStructure,
				expressionIdEqualToProcedureByMethod, insertTime5);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedure,
				insertTime5);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdEvent = new ExpressionId((long) 272379006);
		final ExpressionId conceptIdTravel = new ExpressionId((long) 420008001);
		final ExpressionId conceptIdRecentAirTravel = new ExpressionId((long) 445000002);
		final ExpressionId conceptIdLandslide = new ExpressionId((long) 49061008);
		final ExpressionId conceptIdMudSlide = new ExpressionId((long) 102406006);

		final ExpressionId conceptIdNotableEvent = new ExpressionId((long) 405615003);
		final ExpressionId conceptIdRespiratoryEvent = new ExpressionId((long) 405618001);

		final ExpressionId conceptIdRecordArtifact = new ExpressionId((long) 419891008);

		final ExpressionId expressionIdParentToTravel;
		final ExpressionId expressionIdChildToRecentAirTravel;
		final ExpressionId expressionIdGrandchildToRecentAirTravel;
		final ExpressionId expressionIdLinkingLandslideToRespiratoryEvent;
		try {
			expressionIdParentToTravel = ds.storeExpression("146", null);
			expressionIdChildToRecentAirTravel = ds.storeExpression("147", null);
			expressionIdGrandchildToRecentAirTravel = ds.storeExpression("148", null);
			expressionIdLinkingLandslideToRespiratoryEvent = ds.storeExpression("149", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToTravel, null, children, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdTravel, expressionIdParentToTravel,
				null);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdTravel, expressionIdParentToTravel, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdParentToTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToTravel, expressionIdParentToTravel, null);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToTravel,
				expressionIdChildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToRecentAirTravel,
				expressionIdParentToTravel, null);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToTravel, expressionIdChildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdChildToRecentAirTravel, expressionIdParentToTravel,
				null);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdChildToRecentAirTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToRecentAirTravel,
				expressionIdChildToRecentAirTravel, null);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdTravel, expressionIdGrandchildToRecentAirTravel,
				null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel,
				conceptIdTravel, null);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdTravel, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdTravel,
				null);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdRecordArtifact,
				null);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel,
				expressionIdGrandchildToRecentAirTravel, null);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdEvent, expressionIdGrandchildToRecentAirTravel,
				null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel,
				conceptIdEvent, null);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdEvent, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdEvent, null);

		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);

		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRespiratoryEvent);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdLandslide);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingLandslideToRespiratoryEvent, parents, children,
						null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdLandslide, conceptIdRespiratoryEvent,
				null);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdLandslide, conceptIdRespiratoryEvent, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdRespiratoryEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdRespiratoryEvent, conceptIdRespiratoryEvent, null);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdNotableEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdNotableEvent, conceptIdNotableEvent, null);

		// --------------------------------------------------------------------------------

		final Date insertTime7 = new GregorianCalendar(2113, 03, 03, 11, 19, 22).getTime();
		final Date insertTime8 = new GregorianCalendar(2115, 07, 21, 12, 33, 51).getTime();
		final Date insertTime9 = new GregorianCalendar(2117, 9, 24, 02, 44, 36).getTime();

		final ExpressionId conceptIdOccupation = new ExpressionId((long) 14679004);
		final ExpressionId conceptIdAssembler = new ExpressionId((long) 309036005);
		final ExpressionId conceptIdProductAssembler = new ExpressionId((long) 266048002);
		final ExpressionId conceptIdRepetitiveAssembler = new ExpressionId((long) 308350004);
		final ExpressionId conceptIdRepetitiveAssemblerElectric = new ExpressionId((long) 160117002);

		final ExpressionId conceptIdProfessionalScientistEngineerTechnologist = new ExpressionId((long) 265950004);
		final ExpressionId conceptIdScientist = new ExpressionId((long) 308027003);

		final ExpressionId conceptIdRecordEntry = new ExpressionId((long) 424975005);

		final ExpressionId expressionIdParentToAssembler;
		final ExpressionId expressionIdChildToProductAssembler;
		final ExpressionId expressionIdGrandchildToProductAssembler;
		final ExpressionId expressionIdLinkingRepetitiveAssemblerToScientist;
		try {
			expressionIdParentToAssembler = ds.storeExpression("0140", insertTime8);
			expressionIdChildToProductAssembler = ds.storeExpression("0141", insertTime8);
			expressionIdGrandchildToProductAssembler = ds.storeExpression("0142", insertTime8);
			expressionIdLinkingRepetitiveAssemblerToScientist = ds.storeExpression("0143", insertTime8);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToAssembler, null, children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToProductAssembler, parents, null, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToProductAssembler, parents, null,
						insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdScientist);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdRepetitiveAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingRepetitiveAssemblerToScientist, parents,
						children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseAncestorIdIsInvalidTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdAssembler,
				insertTime7);
		caseDescendantIdIsInvalidTestIsSubsumingNotEquivalent(conceptIdProductAssembler,
				expressionIdChildToProductAssembler, insertTime7);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, expressionIdParentToAssembler,
				insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler,
				expressionIdChildToProductAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime8);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler, conceptIdRecordEntry,
				insertTime8);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdChildToProductAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdAssembler,
				expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				conceptIdAssembler, insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime8);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry,
				insertTime8);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdOccupation,
				expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				conceptIdOccupation, insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist,
				insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime8);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdScientist, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, expressionIdParentToAssembler,
				insertTime9);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdParentToAssembler,
				expressionIdChildToProductAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime9);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler, conceptIdRecordEntry,
				insertTime9);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdChildToProductAssembler,
				expressionIdChildToProductAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdAssembler,
				expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				conceptIdAssembler, insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime9);
		caseNotRelatedTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry,
				insertTime9);
		caseEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdOccupation,
				expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler,
				conceptIdOccupation, insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime9);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist,
				insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime9);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdScientist, conceptIdScientist, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseSubsumingTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingTestIsSubsumingNotEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseNotRelatedTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsumingNotEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
	}

	/**
	 * Test the case "Subsuming not equivalent" to see if the method <code>isSubsumingNotEquivalent</code> works
	 * correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertTrue(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, descendantId, time));
			assertFalse(
					"The concept or expression " + descendantId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, ancestorId, time));
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, ancestorId, time));
			assertFalse(
					"The concept or expression " + descendantId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Inverse subsuming not equivalent" to see if the method <code>isSubsumingNotEquivalent</code> works
	 * correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, descendantId, time));
			assertTrue(
					"The concept or expression " + descendantId.getId().toString()
							+ " should (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, ancestorId, time));
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, ancestorId, time));
			assertFalse(
					"The concept or expression " + descendantId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Subsuming" to see if the method <code>isSubsumingNotEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		caseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "Inverse subsuming" to see if the method <code>isSubsumingNotEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		caseInverseSubsumingNotEquivalentTestIsSubsumingNotEquivalent(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "Not related" to see if the method <code>isSubsumingNotEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseNotRelatedTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, descendantId, time));
			assertFalse(
					"The concept or expression " + descendantId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, ancestorId, time));
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ ancestorId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(ancestorId, ancestorId, time));
			assertFalse(
					"The concept or expression " + descendantId.getId().toString()
							+ " should not (subsume and not be equivalent) to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isSubsumingNotEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Equivalent" to see if the method <code>isSubsumingNotEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseEquivalentTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		caseNotRelatedTestIsSubsumingNotEquivalent(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "<code>ancestorId</code> is invalid" to see if the method <code>isSubsumingNotEquivalent</code>
	 * works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseAncestorIdIsInvalidTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			ds.isSubsumingNotEquivalent(ancestorId, descendantId, time);
			fail("The concept or expression id " + ancestorId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "<code>descendantId</code> is invalid" to see if the method <code>isSubsumingNotEquivalent</code>
	 * works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseDescendantIdIsInvalidTestIsSubsumingNotEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			ds.isSubsumingNotEquivalent(ancestorId, descendantId, time);
			fail("The concept or expression id " + descendantId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isEquivalent(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsEquivalent() {
		final ExpressionId conceptIdBodyStructure = new ExpressionId((long) 123037004);
		final ExpressionId conceptIdProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptIdProcedureByMethod = new ExpressionId((long) 128927009);
		final Date insertTime1 = new GregorianCalendar(2000, 03, 13, 02, 12, 58).getTime();
		final Date insertTime2 = new GregorianCalendar(2003, 01, 21, 11, 55, 44).getTime();

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, null);

		caseSubsumingTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingTestIsEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, null);

		caseNotRelatedTestIsEquivalent(conceptIdBodyStructure, conceptIdProcedureByMethod, null);
		caseEquivalentTestIsEquivalent(conceptIdProcedure, conceptIdProcedure, null);

		caseAncestorIdIsInvalidTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, insertTime1);
		caseDescendantIdIsInvalidTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, insertTime1);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, insertTime2);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, insertTime2);

		caseSubsumingTestIsEquivalent(conceptIdProcedure, conceptIdProcedureByMethod, insertTime2);
		caseInverseSubsumingTestIsEquivalent(conceptIdProcedureByMethod, conceptIdProcedure, insertTime2);

		caseNotRelatedTestIsEquivalent(conceptIdBodyStructure, conceptIdProcedureByMethod, insertTime2);
		caseEquivalentTestIsEquivalent(conceptIdProcedure, conceptIdProcedure, insertTime2);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdOrganism = new ExpressionId((long) 410607006);
		final ExpressionId conceptIdQualifierValue = new ExpressionId((long) 362981000);
		final ExpressionId conceptIdAction = new ExpressionId((long) 129264002);

		final ExpressionId expressionIdEqualToOrganism;
		final ExpressionId expressionIdEqualToQualifierValue;
		final ExpressionId expressionIdEqualToAction;

		try {
			expressionIdEqualToOrganism = ds.storeExpression("140", null);
			ds.storeExpressionEquivalence(expressionIdEqualToOrganism, conceptIdOrganism, null);
			expressionIdEqualToQualifierValue = ds.storeExpression("141", null);
			ds.storeExpressionEquivalence(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
			expressionIdEqualToAction = ds.storeExpression("142", null);
			ds.storeExpressionEquivalence(expressionIdEqualToAction, conceptIdAction, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToQualifierValue, conceptIdAction, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdAction, expressionIdEqualToQualifierValue, null);
		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToAction, conceptIdQualifierValue, null);
		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToAction, expressionIdEqualToQualifierValue,
				null);

		caseSubsumingTestIsEquivalent(expressionIdEqualToQualifierValue, conceptIdAction, null);
		caseInverseSubsumingTestIsEquivalent(conceptIdAction, expressionIdEqualToQualifierValue, null);
		caseSubsumingTestIsEquivalent(conceptIdQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToAction, conceptIdQualifierValue, null);
		caseSubsumingTestIsEquivalent(expressionIdEqualToQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToAction, expressionIdEqualToQualifierValue, null);

		caseNotRelatedTestIsEquivalent(expressionIdEqualToOrganism, conceptIdAction, null);
		caseEquivalentTestIsEquivalent(conceptIdQualifierValue, expressionIdEqualToQualifierValue, null);
		caseNotRelatedTestIsEquivalent(conceptIdOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsEquivalent(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
		caseNotRelatedTestIsEquivalent(expressionIdEqualToOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsEquivalent(expressionIdEqualToQualifierValue, expressionIdEqualToQualifierValue, null);

		// --------------------------------------------------------------------------------

		final Date insertTime3 = new GregorianCalendar(2105, 01, 11, 21, 16, 23).getTime();
		final Date insertTime4 = new GregorianCalendar(2107, 03, 12, 11, 31, 41).getTime();
		final Date insertTime5 = new GregorianCalendar(2111, 03, 01, 06, 13, 06).getTime();

		final ExpressionId expressionIdEqualToBodyStructure;
		final ExpressionId expressionIdEqualToProcedure;
		final ExpressionId expressionIdEqualToProcedureByMethod;

		try {
			expressionIdEqualToBodyStructure = ds.storeExpression("143", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToBodyStructure, conceptIdBodyStructure, insertTime4);
			expressionIdEqualToProcedure = ds.storeExpression("144", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
			expressionIdEqualToProcedureByMethod = ds.storeExpression("145", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedureByMethod, conceptIdProcedureByMethod,
					insertTime4);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseAncestorIdIsInvalidTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime3);
		caseDescendantIdIsInvalidTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime3);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime4);
		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime4);
		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime4);

		caseSubsumingTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure, insertTime4);
		caseSubsumingTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure, insertTime4);
		caseSubsumingTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToProcedureByMethod, expressionIdEqualToProcedure,
				insertTime4);

		caseNotRelatedTestIsEquivalent(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod, insertTime4);
		caseEquivalentTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedure, insertTime4);
		caseNotRelatedTestIsEquivalent(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseEquivalentTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
		caseNotRelatedTestIsEquivalent(expressionIdEqualToBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseEquivalentTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedure, insertTime4);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime5);
		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime5);
		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime5);

		caseSubsumingTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsEquivalent(conceptIdProcedureByMethod, expressionIdEqualToProcedure, insertTime5);
		caseSubsumingTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToProcedureByMethod, conceptIdProcedure, insertTime5);
		caseSubsumingTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsEquivalent(expressionIdEqualToProcedureByMethod, expressionIdEqualToProcedure,
				insertTime5);

		caseNotRelatedTestIsEquivalent(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod, insertTime5);
		caseEquivalentTestIsEquivalent(conceptIdProcedure, expressionIdEqualToProcedure, insertTime5);
		caseNotRelatedTestIsEquivalent(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseEquivalentTestIsEquivalent(expressionIdEqualToProcedure, conceptIdProcedure, insertTime5);
		caseNotRelatedTestIsEquivalent(expressionIdEqualToBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseEquivalentTestIsEquivalent(expressionIdEqualToProcedure, expressionIdEqualToProcedure, insertTime5);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdEvent = new ExpressionId((long) 272379006);
		final ExpressionId conceptIdTravel = new ExpressionId((long) 420008001);
		final ExpressionId conceptIdRecentAirTravel = new ExpressionId((long) 445000002);
		final ExpressionId conceptIdLandslide = new ExpressionId((long) 49061008);
		final ExpressionId conceptIdMudSlide = new ExpressionId((long) 102406006);

		final ExpressionId conceptIdNotableEvent = new ExpressionId((long) 405615003);
		final ExpressionId conceptIdRespiratoryEvent = new ExpressionId((long) 405618001);

		final ExpressionId conceptIdRecordArtifact = new ExpressionId((long) 419891008);

		final ExpressionId expressionIdParentToTravel;
		final ExpressionId expressionIdChildToRecentAirTravel;
		final ExpressionId expressionIdGrandchildToRecentAirTravel;
		final ExpressionId expressionIdLinkingLandslideToRespiratoryEvent;
		try {
			expressionIdParentToTravel = ds.storeExpression("146", null);
			expressionIdChildToRecentAirTravel = ds.storeExpression("147", null);
			expressionIdGrandchildToRecentAirTravel = ds.storeExpression("148", null);
			expressionIdLinkingLandslideToRespiratoryEvent = ds.storeExpression("149", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToTravel, null, children, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdTravel, expressionIdParentToTravel, null);
		caseSubsumingTestIsEquivalent(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingTestIsEquivalent(conceptIdTravel, expressionIdParentToTravel, null);
		caseNotRelatedTestIsEquivalent(expressionIdParentToTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsEquivalent(expressionIdParentToTravel, expressionIdParentToTravel, null);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToTravel, expressionIdChildToRecentAirTravel,
				null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdChildToRecentAirTravel,
				expressionIdParentToTravel, null);
		caseSubsumingTestIsEquivalent(expressionIdParentToTravel, expressionIdChildToRecentAirTravel, null);
		caseInverseSubsumingTestIsEquivalent(expressionIdChildToRecentAirTravel, expressionIdParentToTravel, null);
		caseNotRelatedTestIsEquivalent(expressionIdChildToRecentAirTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsEquivalent(expressionIdChildToRecentAirTravel, expressionIdChildToRecentAirTravel, null);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdTravel, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdTravel,
				null);
		caseSubsumingTestIsEquivalent(conceptIdTravel, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdTravel, null);
		caseNotRelatedTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, expressionIdGrandchildToRecentAirTravel,
				null);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdEvent, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdEvent,
				null);
		caseSubsumingTestIsEquivalent(conceptIdEvent, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToRecentAirTravel, conceptIdEvent, null);

		caseNotRelatedTestIsEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseNotRelatedTestIsEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);

		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRespiratoryEvent);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdLandslide);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingLandslideToRespiratoryEvent, parents, children,
						null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdLandslide, conceptIdRespiratoryEvent, null);
		caseSubsumingTestIsEquivalent(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingTestIsEquivalent(conceptIdLandslide, conceptIdRespiratoryEvent, null);
		caseNotRelatedTestIsEquivalent(conceptIdRespiratoryEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsEquivalent(conceptIdRespiratoryEvent, conceptIdRespiratoryEvent, null);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseSubsumingTestIsEquivalent(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingTestIsEquivalent(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseNotRelatedTestIsEquivalent(conceptIdNotableEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsEquivalent(conceptIdNotableEvent, conceptIdNotableEvent, null);

		// --------------------------------------------------------------------------------

		final Date insertTime7 = new GregorianCalendar(2113, 03, 03, 11, 19, 22).getTime();
		final Date insertTime8 = new GregorianCalendar(2115, 07, 21, 12, 33, 51).getTime();
		final Date insertTime9 = new GregorianCalendar(2117, 9, 24, 02, 44, 36).getTime();

		final ExpressionId conceptIdOccupation = new ExpressionId((long) 14679004);
		final ExpressionId conceptIdAssembler = new ExpressionId((long) 309036005);
		final ExpressionId conceptIdProductAssembler = new ExpressionId((long) 266048002);
		final ExpressionId conceptIdRepetitiveAssembler = new ExpressionId((long) 308350004);
		final ExpressionId conceptIdRepetitiveAssemblerElectric = new ExpressionId((long) 160117002);

		final ExpressionId conceptIdProfessionalScientistEngineerTechnologist = new ExpressionId((long) 265950004);
		final ExpressionId conceptIdScientist = new ExpressionId((long) 308027003);

		final ExpressionId conceptIdRecordEntry = new ExpressionId((long) 424975005);

		final ExpressionId expressionIdParentToAssembler;
		final ExpressionId expressionIdChildToProductAssembler;
		final ExpressionId expressionIdGrandchildToProductAssembler;
		final ExpressionId expressionIdLinkingRepetitiveAssemblerToScientist;
		try {
			expressionIdParentToAssembler = ds.storeExpression("0140", insertTime8);
			expressionIdChildToProductAssembler = ds.storeExpression("0141", insertTime8);
			expressionIdGrandchildToProductAssembler = ds.storeExpression("0142", insertTime8);
			expressionIdLinkingRepetitiveAssemblerToScientist = ds.storeExpression("0143", insertTime8);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToAssembler, null, children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToProductAssembler, parents, null, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToProductAssembler, parents, null,
						insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdScientist);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdRepetitiveAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingRepetitiveAssemblerToScientist, parents,
						children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseAncestorIdIsInvalidTestIsEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime7);
		caseDescendantIdIsInvalidTestIsEquivalent(conceptIdProductAssembler, expressionIdChildToProductAssembler,
				insertTime7);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseSubsumingTestIsEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime8);
		caseInverseSubsumingTestIsEquivalent(conceptIdAssembler, expressionIdParentToAssembler, insertTime8);
		caseNotRelatedTestIsEquivalent(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsEquivalent(expressionIdParentToAssembler, expressionIdParentToAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime8);
		caseSubsumingTestIsEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsEquivalent(expressionIdChildToProductAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseNotRelatedTestIsEquivalent(expressionIdChildToProductAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsEquivalent(expressionIdChildToProductAssembler, expressionIdChildToProductAssembler,
				insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime8);
		caseSubsumingTestIsEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler, insertTime8);
		caseNotRelatedTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime8);
		caseSubsumingTestIsEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist,
				insertTime8);
		caseSubsumingTestIsEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime8);
		caseInverseSubsumingTestIsEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime8);
		caseNotRelatedTestIsEquivalent(conceptIdScientist, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsEquivalent(conceptIdScientist, conceptIdScientist, insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseSubsumingTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingTestIsEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseNotRelatedTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist, conceptIdRecordEntry,
				insertTime8);
		caseEquivalentTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseSubsumingTestIsEquivalent(expressionIdParentToAssembler, conceptIdAssembler, insertTime9);
		caseInverseSubsumingTestIsEquivalent(conceptIdAssembler, expressionIdParentToAssembler, insertTime9);
		caseNotRelatedTestIsEquivalent(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsEquivalent(expressionIdParentToAssembler, expressionIdParentToAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime9);
		caseSubsumingTestIsEquivalent(expressionIdParentToAssembler, expressionIdChildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsEquivalent(expressionIdChildToProductAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseNotRelatedTestIsEquivalent(expressionIdChildToProductAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsEquivalent(expressionIdChildToProductAssembler, expressionIdChildToProductAssembler,
				insertTime9);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime9);
		caseSubsumingTestIsEquivalent(conceptIdAssembler, expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdAssembler, insertTime9);
		caseNotRelatedTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime9);
		caseSubsumingTestIsEquivalent(conceptIdOccupation, expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsEquivalent(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime9);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist,
				insertTime9);
		caseSubsumingTestIsEquivalent(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime9);
		caseInverseSubsumingTestIsEquivalent(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime9);
		caseNotRelatedTestIsEquivalent(conceptIdScientist, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsEquivalent(conceptIdScientist, conceptIdScientist, insertTime9);

		caseSubsumingNotEquivalentTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseSubsumingTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingTestIsEquivalent(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseNotRelatedTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist, conceptIdRecordEntry,
				insertTime9);
		caseEquivalentTestIsEquivalent(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
	}

	/**
	 * Test the case "Subsuming not equivalent" to see if the method <code>isEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingNotEquivalentTestIsEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not be equivalent to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isEquivalent(ancestorId, descendantId, time));
			assertFalse("The concept or expression " + descendantId.getId().toString()
					+ " should not be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should be equivalent to the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Inverse subsuming not equivalent" to see if the method <code>isEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingNotEquivalentTestIsEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		caseSubsumingNotEquivalentTestIsEquivalent(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "Subsuming" is meaningless for the method <code>isEquivalent</code>, but it is kept to keep the
	 * symmetry to the other test methods.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingTestIsEquivalent(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
	}

	/**
	 * Test the case "Inverse subsuming" is meaningless for the method <code>isEquivalent</code>, but it is kept to to
	 * correspond to the other test methods.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingTestIsEquivalent(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
	}

	/**
	 * Test the case "Not related" to see if the method <code>isEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseNotRelatedTestIsEquivalent(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			assertFalse(
					"The concept or expression " + ancestorId.getId().toString()
							+ " should not be equivalent to the concept or expression "
							+ descendantId.getId().toString() + " at the time " + time + ".",
					ds.isEquivalent(ancestorId, descendantId, time));
			assertFalse("The concept or expression " + descendantId.getId().toString()
					+ " should not be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should be equivalent to the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Equivalent" to see if the method <code>isEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseEquivalentTestIsEquivalent(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should be equivalent to the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(ancestorId, descendantId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should be equivalent to the concept or expression " + ancestorId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should be equivalent to the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "<code>ancestorId</code> is invalid" to see if the method <code>isEquivalent</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseAncestorIdIsInvalidTestIsEquivalent(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			ds.isEquivalent(ancestorId, descendantId, time);
			fail("The concept or expression id " + ancestorId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "<code>descendantId</code> is invalid" to see if the method <code>isEquivalent</code> works
	 * correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseDescendantIdIsInvalidTestIsEquivalent(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			ds.isEquivalent(ancestorId, descendantId, time);
			fail("The concept or expression id " + descendantId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isSubsuming(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId,java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsSubsuming() {
		final ExpressionId conceptIdBodyStructure = new ExpressionId((long) 123037004);
		final ExpressionId conceptIdProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptIdProcedureByMethod = new ExpressionId((long) 128927009);
		final Date insertTime1 = new GregorianCalendar(2000, 03, 13, 02, 12, 58).getTime();
		final Date insertTime2 = new GregorianCalendar(2003, 01, 21, 11, 55, 44).getTime();

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedureByMethod, conceptIdProcedure, null);

		caseSubsumingTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, null);
		caseInverseSubsumingTestIsSubsuming(conceptIdProcedureByMethod, conceptIdProcedure, null);

		caseNotRelatedTestIsSubsuming(conceptIdBodyStructure, conceptIdProcedureByMethod, null);
		caseEquivalentTestIsSubsuming(conceptIdProcedure, conceptIdProcedure, null);

		caseAncestorIdIsInvalidTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, insertTime1);
		caseDescendantIdIsInvalidTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, insertTime1);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, insertTime2);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedureByMethod, conceptIdProcedure, insertTime2);

		caseSubsumingTestIsSubsuming(conceptIdProcedure, conceptIdProcedureByMethod, insertTime2);
		caseInverseSubsumingTestIsSubsuming(conceptIdProcedureByMethod, conceptIdProcedure, insertTime2);

		caseNotRelatedTestIsSubsuming(conceptIdBodyStructure, conceptIdProcedureByMethod, insertTime2);
		caseEquivalentTestIsSubsuming(conceptIdProcedure, conceptIdProcedure, insertTime2);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdOrganism = new ExpressionId((long) 410607006);
		final ExpressionId conceptIdQualifierValue = new ExpressionId((long) 362981000);
		final ExpressionId conceptIdAction = new ExpressionId((long) 129264002);

		final ExpressionId expressionIdEqualToOrganism;
		final ExpressionId expressionIdEqualToQualifierValue;
		final ExpressionId expressionIdEqualToAction;

		try {
			expressionIdEqualToOrganism = ds.storeExpression("140", null);
			ds.storeExpressionEquivalence(expressionIdEqualToOrganism, conceptIdOrganism, null);
			expressionIdEqualToQualifierValue = ds.storeExpression("141", null);
			ds.storeExpressionEquivalence(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
			expressionIdEqualToAction = ds.storeExpression("142", null);
			ds.storeExpressionEquivalence(expressionIdEqualToAction, conceptIdAction, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToQualifierValue, conceptIdAction, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdAction, expressionIdEqualToQualifierValue, null);
		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToAction, conceptIdQualifierValue, null);
		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToAction, expressionIdEqualToQualifierValue,
				null);

		caseSubsumingTestIsSubsuming(expressionIdEqualToQualifierValue, conceptIdAction, null);
		caseInverseSubsumingTestIsSubsuming(conceptIdAction, expressionIdEqualToQualifierValue, null);
		caseSubsumingTestIsSubsuming(conceptIdQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToAction, conceptIdQualifierValue, null);
		caseSubsumingTestIsSubsuming(expressionIdEqualToQualifierValue, expressionIdEqualToAction, null);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToAction, expressionIdEqualToQualifierValue, null);

		caseNotRelatedTestIsSubsuming(expressionIdEqualToOrganism, conceptIdAction, null);
		caseEquivalentTestIsSubsuming(conceptIdQualifierValue, expressionIdEqualToQualifierValue, null);
		caseNotRelatedTestIsSubsuming(conceptIdOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsSubsuming(expressionIdEqualToQualifierValue, conceptIdQualifierValue, null);
		caseNotRelatedTestIsSubsuming(expressionIdEqualToOrganism, expressionIdEqualToAction, null);
		caseEquivalentTestIsSubsuming(expressionIdEqualToQualifierValue, expressionIdEqualToQualifierValue, null);

		// --------------------------------------------------------------------------------

		final Date insertTime3 = new GregorianCalendar(2105, 01, 11, 21, 16, 23).getTime();
		final Date insertTime4 = new GregorianCalendar(2107, 03, 12, 11, 31, 41).getTime();
		final Date insertTime5 = new GregorianCalendar(2111, 03, 01, 06, 13, 06).getTime();

		final ExpressionId expressionIdEqualToBodyStructure;
		final ExpressionId expressionIdEqualToProcedure;
		final ExpressionId expressionIdEqualToProcedureByMethod;

		try {
			expressionIdEqualToBodyStructure = ds.storeExpression("143", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToBodyStructure, conceptIdBodyStructure, insertTime4);
			expressionIdEqualToProcedure = ds.storeExpression("144", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
			expressionIdEqualToProcedureByMethod = ds.storeExpression("145", insertTime4);
			ds.storeExpressionEquivalence(expressionIdEqualToProcedureByMethod, conceptIdProcedureByMethod,
					insertTime4);
		} catch (DataStoreException | ExpressionAlreadyExistsException | NonExistingIdException
				| ExpressionAlreadyDefined e) {
			throw new AssertionError(e);
		}

		caseAncestorIdIsInvalidTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime3);
		caseDescendantIdIsInvalidTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedureByMethod, insertTime3);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime4);
		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime4);
		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime4);

		caseSubsumingTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsSubsuming(conceptIdProcedureByMethod, expressionIdEqualToProcedure, insertTime4);
		caseSubsumingTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToProcedureByMethod, conceptIdProcedure, insertTime4);
		caseSubsumingTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToProcedureByMethod, expressionIdEqualToProcedure,
				insertTime4);

		caseNotRelatedTestIsSubsuming(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod, insertTime4);
		caseEquivalentTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedure, insertTime4);
		caseNotRelatedTestIsSubsuming(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod, insertTime4);
		caseEquivalentTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedure, insertTime4);
		caseNotRelatedTestIsSubsuming(expressionIdEqualToBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime4);
		caseEquivalentTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedure, insertTime4);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedureByMethod, expressionIdEqualToProcedure,
				insertTime5);
		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedureByMethod, conceptIdProcedure,
				insertTime5);
		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdEqualToProcedureByMethod,
				expressionIdEqualToProcedure, insertTime5);

		caseSubsumingTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsSubsuming(conceptIdProcedureByMethod, expressionIdEqualToProcedure, insertTime5);
		caseSubsumingTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToProcedureByMethod, conceptIdProcedure, insertTime5);
		caseSubsumingTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseInverseSubsumingTestIsSubsuming(expressionIdEqualToProcedureByMethod, expressionIdEqualToProcedure,
				insertTime5);

		caseNotRelatedTestIsSubsuming(expressionIdEqualToBodyStructure, conceptIdProcedureByMethod, insertTime5);
		caseEquivalentTestIsSubsuming(conceptIdProcedure, expressionIdEqualToProcedure, insertTime5);
		caseNotRelatedTestIsSubsuming(conceptIdBodyStructure, expressionIdEqualToProcedureByMethod, insertTime5);
		caseEquivalentTestIsSubsuming(expressionIdEqualToProcedure, conceptIdProcedure, insertTime5);
		caseNotRelatedTestIsSubsuming(expressionIdEqualToBodyStructure, expressionIdEqualToProcedureByMethod,
				insertTime5);
		caseEquivalentTestIsSubsuming(expressionIdEqualToProcedure, expressionIdEqualToProcedure, insertTime5);

		// --------------------------------------------------------------------------------

		final ExpressionId conceptIdEvent = new ExpressionId((long) 272379006);
		final ExpressionId conceptIdTravel = new ExpressionId((long) 420008001);
		final ExpressionId conceptIdRecentAirTravel = new ExpressionId((long) 445000002);
		final ExpressionId conceptIdLandslide = new ExpressionId((long) 49061008);
		final ExpressionId conceptIdMudSlide = new ExpressionId((long) 102406006);

		final ExpressionId conceptIdNotableEvent = new ExpressionId((long) 405615003);
		final ExpressionId conceptIdRespiratoryEvent = new ExpressionId((long) 405618001);

		final ExpressionId conceptIdRecordArtifact = new ExpressionId((long) 419891008);

		final ExpressionId expressionIdParentToTravel;
		final ExpressionId expressionIdChildToRecentAirTravel;
		final ExpressionId expressionIdGrandchildToRecentAirTravel;
		final ExpressionId expressionIdLinkingLandslideToRespiratoryEvent;
		try {
			expressionIdParentToTravel = ds.storeExpression("146", null);
			expressionIdChildToRecentAirTravel = ds.storeExpression("147", null);
			expressionIdGrandchildToRecentAirTravel = ds.storeExpression("148", null);
			expressionIdLinkingLandslideToRespiratoryEvent = ds.storeExpression("149", null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToTravel, null, children, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToRecentAirTravel);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToRecentAirTravel, parents, null, null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdTravel, expressionIdParentToTravel, null);
		caseSubsumingTestIsSubsuming(expressionIdParentToTravel, conceptIdTravel, null);
		caseInverseSubsumingTestIsSubsuming(conceptIdTravel, expressionIdParentToTravel, null);
		caseNotRelatedTestIsSubsuming(expressionIdParentToTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsuming(expressionIdParentToTravel, expressionIdParentToTravel, null);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToTravel, expressionIdChildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdChildToRecentAirTravel, expressionIdParentToTravel,
				null);
		caseSubsumingTestIsSubsuming(expressionIdParentToTravel, expressionIdChildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsuming(expressionIdChildToRecentAirTravel, expressionIdParentToTravel, null);
		caseNotRelatedTestIsSubsuming(expressionIdChildToRecentAirTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsuming(expressionIdChildToRecentAirTravel, expressionIdChildToRecentAirTravel, null);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdTravel, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, conceptIdTravel,
				null);
		caseSubsumingTestIsSubsuming(conceptIdTravel, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, conceptIdTravel, null);
		caseNotRelatedTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, expressionIdGrandchildToRecentAirTravel,
				null);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdEvent, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, conceptIdEvent, null);
		caseSubsumingTestIsSubsuming(conceptIdEvent, expressionIdGrandchildToRecentAirTravel, null);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToRecentAirTravel, conceptIdEvent, null);

		caseNotRelatedTestIsSubsuming(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseNotRelatedTestIsSubsuming(conceptIdNotableEvent, conceptIdMudSlide, null);

		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdRespiratoryEvent);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdLandslide);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingLandslideToRespiratoryEvent, parents, children,
						null);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdLandslide, conceptIdRespiratoryEvent, null);
		caseSubsumingTestIsSubsuming(conceptIdRespiratoryEvent, conceptIdLandslide, null);
		caseInverseSubsumingTestIsSubsuming(conceptIdLandslide, conceptIdRespiratoryEvent, null);
		caseNotRelatedTestIsSubsuming(conceptIdRespiratoryEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsuming(conceptIdRespiratoryEvent, conceptIdRespiratoryEvent, null);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseSubsumingTestIsSubsuming(conceptIdNotableEvent, conceptIdMudSlide, null);
		caseInverseSubsumingTestIsSubsuming(conceptIdMudSlide, conceptIdNotableEvent, null);
		caseNotRelatedTestIsSubsuming(conceptIdNotableEvent, conceptIdRecordArtifact, null);
		caseEquivalentTestIsSubsuming(conceptIdNotableEvent, conceptIdNotableEvent, null);

		// --------------------------------------------------------------------------------

		final Date insertTime7 = new GregorianCalendar(2113, 03, 03, 11, 19, 22).getTime();
		final Date insertTime8 = new GregorianCalendar(2115, 07, 21, 12, 33, 51).getTime();
		final Date insertTime9 = new GregorianCalendar(2117, 9, 24, 02, 44, 36).getTime();

		final ExpressionId conceptIdOccupation = new ExpressionId((long) 14679004);
		final ExpressionId conceptIdAssembler = new ExpressionId((long) 309036005);
		final ExpressionId conceptIdProductAssembler = new ExpressionId((long) 266048002);
		final ExpressionId conceptIdRepetitiveAssembler = new ExpressionId((long) 308350004);
		final ExpressionId conceptIdRepetitiveAssemblerElectric = new ExpressionId((long) 160117002);

		final ExpressionId conceptIdProfessionalScientistEngineerTechnologist = new ExpressionId((long) 265950004);
		final ExpressionId conceptIdScientist = new ExpressionId((long) 308027003);

		final ExpressionId conceptIdRecordEntry = new ExpressionId((long) 424975005);

		final ExpressionId expressionIdParentToAssembler;
		final ExpressionId expressionIdChildToProductAssembler;
		final ExpressionId expressionIdGrandchildToProductAssembler;
		final ExpressionId expressionIdLinkingRepetitiveAssemblerToScientist;
		try {
			expressionIdParentToAssembler = ds.storeExpression("0140", insertTime8);
			expressionIdChildToProductAssembler = ds.storeExpression("0141", insertTime8);
			expressionIdGrandchildToProductAssembler = ds.storeExpression("0142", insertTime8);
			expressionIdLinkingRepetitiveAssemblerToScientist = ds.storeExpression("0143", insertTime8);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		{
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdParentToAssembler, null, children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdChildToProductAssembler, parents, null, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(expressionIdChildToProductAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdGrandchildToProductAssembler, parents, null,
						insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}
		{
			final Set<ExpressionId> parents = new HashSet<ExpressionId>();
			parents.add(conceptIdScientist);
			final Set<ExpressionId> children = new HashSet<ExpressionId>();
			children.add(conceptIdRepetitiveAssembler);
			try {
				ds.storeExpressionParentsAndChildren(expressionIdLinkingRepetitiveAssemblerToScientist, parents,
						children, insertTime8);
			} catch (DataStoreException | NonExistingIdException | ExpressionAlreadyDefined e) {
				throw new AssertionError(e);
			}
		}

		caseAncestorIdIsInvalidTestIsSubsuming(expressionIdParentToAssembler, conceptIdAssembler, insertTime7);
		caseDescendantIdIsInvalidTestIsSubsuming(conceptIdProductAssembler, expressionIdChildToProductAssembler,
				insertTime7);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToAssembler, conceptIdAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseSubsumingTestIsSubsuming(expressionIdParentToAssembler, conceptIdAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsuming(conceptIdAssembler, expressionIdParentToAssembler, insertTime8);
		caseNotRelatedTestIsSubsuming(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsuming(expressionIdParentToAssembler, expressionIdParentToAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime8);
		caseSubsumingTestIsSubsuming(expressionIdParentToAssembler, expressionIdChildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsuming(expressionIdChildToProductAssembler, expressionIdParentToAssembler,
				insertTime8);
		caseNotRelatedTestIsSubsuming(expressionIdChildToProductAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsuming(expressionIdChildToProductAssembler, expressionIdChildToProductAssembler,
				insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime8);
		caseSubsumingTestIsSubsuming(conceptIdAssembler, expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdAssembler, insertTime8);
		caseNotRelatedTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime8);
		caseSubsumingTestIsSubsuming(conceptIdOccupation, expressionIdGrandchildToProductAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdOccupation, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime8);
		caseSubsumingTestIsSubsuming(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime8);
		caseInverseSubsumingTestIsSubsuming(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime8);
		caseNotRelatedTestIsSubsuming(conceptIdScientist, conceptIdRecordEntry, insertTime8);
		caseEquivalentTestIsSubsuming(conceptIdScientist, conceptIdScientist, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseSubsumingTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime8);
		caseInverseSubsumingTestIsSubsuming(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);
		caseNotRelatedTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist, conceptIdRecordEntry,
				insertTime8);
		caseEquivalentTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime8);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToAssembler, conceptIdAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseSubsumingTestIsSubsuming(expressionIdParentToAssembler, conceptIdAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsuming(conceptIdAssembler, expressionIdParentToAssembler, insertTime9);
		caseNotRelatedTestIsSubsuming(expressionIdParentToAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsuming(expressionIdParentToAssembler, expressionIdParentToAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsuming(expressionIdParentToAssembler, expressionIdChildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdChildToProductAssembler,
				expressionIdParentToAssembler, insertTime9);
		caseSubsumingTestIsSubsuming(expressionIdParentToAssembler, expressionIdChildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsuming(expressionIdChildToProductAssembler, expressionIdParentToAssembler,
				insertTime9);
		caseNotRelatedTestIsSubsuming(expressionIdChildToProductAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsuming(expressionIdChildToProductAssembler, expressionIdChildToProductAssembler,
				insertTime9);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdAssembler, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdAssembler,
				insertTime9);
		caseSubsumingTestIsSubsuming(conceptIdAssembler, expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdAssembler, insertTime9);
		caseNotRelatedTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler,
				expressionIdGrandchildToProductAssembler, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdOccupation, expressionIdGrandchildToProductAssembler,
				insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdOccupation,
				insertTime9);
		caseSubsumingTestIsSubsuming(conceptIdOccupation, expressionIdGrandchildToProductAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsuming(expressionIdGrandchildToProductAssembler, conceptIdOccupation, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime9);
		caseSubsumingTestIsSubsuming(conceptIdScientist, conceptIdRepetitiveAssembler, insertTime9);
		caseInverseSubsumingTestIsSubsuming(conceptIdRepetitiveAssembler, conceptIdScientist, insertTime9);
		caseNotRelatedTestIsSubsuming(conceptIdScientist, conceptIdRecordEntry, insertTime9);
		caseEquivalentTestIsSubsuming(conceptIdScientist, conceptIdScientist, insertTime9);

		caseSubsumingNotEquivalentTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingNotEquivalentTestIsSubsuming(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseSubsumingTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdRepetitiveAssemblerElectric, insertTime9);
		caseInverseSubsumingTestIsSubsuming(conceptIdRepetitiveAssemblerElectric,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
		caseNotRelatedTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist, conceptIdRecordEntry,
				insertTime9);
		caseEquivalentTestIsSubsuming(conceptIdProfessionalScientistEngineerTechnologist,
				conceptIdProfessionalScientistEngineerTechnologist, insertTime9);
	}

	/**
	 * Test the case "Subsuming not equivalent" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingNotEquivalentTestIsSubsuming(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(ancestorId, descendantId, time));
			assertFalse("The concept or expression " + descendantId.getId().toString()
					+ " should not subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Inverse subsuming not equivalent" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingNotEquivalentTestIsSubsuming(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			assertFalse("The concept or expression " + ancestorId.getId().toString()
					+ " should not subsume the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isSubsuming(ancestorId, descendantId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Subsuming" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseSubsumingTestIsSubsuming(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		caseSubsumingNotEquivalentTestIsSubsuming(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "Inverse subsuming" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseInverseSubsumingTestIsSubsuming(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		caseInverseSubsumingNotEquivalentTestIsSubsuming(ancestorId, descendantId, time);
	}

	/**
	 * Test the case "Not related" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseNotRelatedTestIsSubsuming(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			assertFalse("The concept or expression " + ancestorId.getId().toString()
					+ " should not subsume the concept or expression " + descendantId.getId().toString()
					+ " at the time " + time + ".", ds.isSubsuming(ancestorId, descendantId, time));
			assertFalse("The concept or expression " + descendantId.getId().toString()
					+ " should not subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isEquivalent(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isEquivalent(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "Equivalent" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseEquivalentTestIsSubsuming(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(ancestorId, descendantId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, ancestorId, time));
			assertTrue("The concept or expression " + ancestorId.getId().toString()
					+ " should subsume the concept or expression " + ancestorId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(ancestorId, ancestorId, time));
			assertTrue("The concept or expression " + descendantId.getId().toString()
					+ " should subsume the concept or expression " + descendantId.getId().toString() + " at the time "
					+ time + ".", ds.isSubsuming(descendantId, descendantId, time));
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "<code>ancestorId</code> is invalid" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseAncestorIdIsInvalidTestIsSubsuming(final ExpressionId ancestorId, final ExpressionId descendantId,
			final Date time) throws AssertionError {
		try {
			ds.isSubsuming(ancestorId, descendantId, time);
			fail("The concept or expression id " + ancestorId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test the case "<code>descendantId</code> is invalid" to see if the method <code>isSubsuming</code> works correct.
	 * 
	 * @param ancestorId
	 *            The id of the concept or expression that is potentially an ancestor.
	 * @param descendantId
	 *            The id of the concept or expression that is potentially a descendant.
	 * @param time
	 *            The specific time. A <code>null</code> value is handled as the current time.
	 * @throws AssertionError
	 *             Thrown if the tests shows an error.
	 */
	private void caseDescendantIdIsInvalidTestIsSubsuming(final ExpressionId ancestorId,
			final ExpressionId descendantId, final Date time) throws AssertionError {
		try {
			ds.isSubsuming(ancestorId, descendantId, time);
			fail("The concept or expression id " + descendantId + " is not a valid id at the time " + time
					+ ", so a NonExistingIdException should have been thrown.");
		} catch (NonExistingIdException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Convert a <code>Date</code> to a <code>String</code> suitable to use in SQL queries.
	 * 
	 * @param date
	 *            The date to convert.
	 * @return The converted date.
	 */
	@SuppressWarnings("deprecation")
	private String toSQL(Date date) {
		return (1900 + date.getYear()) + "-" + (date.getMonth() <= 8 ? "0" : "") + (1 + date.getMonth()) + "-"
				+ (date.getDate() <= 9 ? "0" : "") + date.getDate() + " " + (date.getHours() <= 9 ? "0" : "")
				+ date.getHours() + ":" + (date.getMinutes() <= 9 ? "0" : "") + date.getMinutes() + ":"
				+ (date.getSeconds() <= 9 ? "0" : "") + date.getSeconds();
	}
}
