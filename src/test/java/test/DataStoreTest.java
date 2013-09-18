/**
 * 
 */
package test;

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

import se.liu.imt.mi.snomedct.expressionrepository.api.RelativeAlreadySetException;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionAlreadyExistsException;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStoreService;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * 
 * JUnit test for class
 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore}
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
	 * A <code>Connection</code> to use for preparation before and clean up
	 * after the tests.
	 */
	private static Connection con = null;
	/**
	 * A <code>Statement</code> to use for preparation before and clean up after
	 * the tests.
	 */
	private static Statement stmt = null;

	/**
	 * The test's start time.
	 */
	private static Date startTime = null;;

	/**
	 * The class to test.
	 */
	private DataStore ds;

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

		// Set up a database connection for the test methods to use for
		// preparation before and clean up after the tests.
		Class.forName("org.postgresql.Driver");
		con = DriverManager.getConnection(url, username, password);

		stmt = con.createStatement();
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is and problems with the database.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// Fetch the databas connection details.
		// Clean up the database.
		DataStoreService ds = new DataStoreService(url, username, password);
		ds.restoreDataStore(startTime);
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is and problems with the database.
	 */
	@Before
	public void setUp() throws Exception {
		// Fetch the database connection details.
		ds = new DataStore(url, username, password);
	}

	/**
	 * @throws java.lang.Exception
	 *             If it is and problems with the database.
	 */
	@After
	public void tearDown() throws Exception {
		try {
			ds.finalize();
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#DataStore(java.lang.String, java.lang.String, java.lang.String)}
	 * .
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
	public final void testStoreExpressionWithoutDate() {
		final String expressionWithoutDate = "10";
		final ExpressionId expressionWithoutDateId;
		final ExpressionId expressionWithoutDateIdTested;

		try {
			expressionWithoutDateId = ds.storeExpression(expressionWithoutDate,
					null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		assertNotNull(
				"No ExpressionId returned after expression insertation without date.",
				expressionWithoutDateId);
		assertTrue(
				"The returned ExpressionId after expression insertation without date, "
						+ expressionWithoutDateId.getId().toString()
						+ ", is invalid.", expressionWithoutDateId.getId() < 0);

		try {
			final ResultSet rs1 = stmt
					.executeQuery("SELECT id FROM expressions WHERE expression = '"
							+ expressionWithoutDate + "' AND endtime IS NULL;");

			assertTrue("No stored id for the expression "
					+ expressionWithoutDate + " in the database.", rs1.next());
			expressionWithoutDateIdTested = new ExpressionId(rs1.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertTrue("The stored id for the expression " + expressionWithoutDate
				+ " without date is incorrect in the database.",
				expressionWithoutDateId.equals(expressionWithoutDateIdTested));

		try {
			ds.storeExpression(expressionWithoutDate, null);
			fail("A ExpressionAlreadyExistsException should be thrown when inserting multiple versions of the same expression.");
		} catch (ExpressionAlreadyExistsException e) {
			// Everything is correct.
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpression(java.lang.String, java.util.Date)}
	 * .
	 */
	@Test
	public final void testStoreExpressionWithDate() {
		final String expressionWithDate = "20";
		final ExpressionId expressionWithDateId;
		final ExpressionId expressionWithDateIdTested;
		final Date insertTime = new GregorianCalendar(2110, 12, 03, 16, 14, 32)
				.getTime();

		try {
			expressionWithDateId = ds.storeExpression(expressionWithDate,
					insertTime);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}
		assertNotNull(
				"No ExpressionId returned after expression insertation with date.",
				expressionWithDateId);
		assertTrue(
				"The returned ExpressionId after expression insertation with date, "
						+ expressionWithDateId.getId().toString()
						+ ", is invalid.", expressionWithDateId.getId() < 0);

		try {
			final ResultSet rs2 = stmt
					.executeQuery("SELECT id FROM expressions WHERE expression = '"
							+ expressionWithDate
							+ "' AND starttime = '"
							+ toSQL(insertTime) + "';");

			assertTrue("No stored id for the expression " + expressionWithDate
					+ " at the time " + insertTime + " in the database.",
					rs2.next());
			expressionWithDateIdTested = new ExpressionId(rs2.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertTrue("The stored id for the expression " + expressionWithDate
				+ " at the time " + insertTime
				+ " is incorrect in the database.",
				expressionWithDateId.equals(expressionWithDateIdTested));

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
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpressionEquivalence(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)}
	 * .
	 */
	@Test
	public final void testStoreExpressionEquivalence() {

		final String expression1 = "31";
		final String expression2 = "32";
		final String expression3 = "33";
		final String expression4 = "34";
		final Date insertTime = new GregorianCalendar(2111, 8, 14, 7, 1, 45)
				.getTime();
		final ExpressionId expression1Id;
		final ExpressionId expression2Id;
		final ExpressionId expression3Id;
		final ExpressionId expression4Id;
		final ExpressionId conceptKingdomAnimalia = new ExpressionId(
				(long) 387961004);
		final ExpressionId expression1IdEquivalent;
		final ExpressionId expression2IdEquivalent;
		final Set<ExpressionId> parent = new HashSet<ExpressionId>();
		final Set<ExpressionId> child = new HashSet<ExpressionId>();

		try {
			expression1Id = ds.storeExpression(expression1, null);
			expression2Id = ds.storeExpression(expression2, insertTime);
			expression3Id = ds.storeExpression(expression3, null);
			expression4Id = ds.storeExpression(expression4, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expression1Id, conceptKingdomAnimalia);
			ds.storeExpressionEquivalence(expression2Id, expression1Id);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException e) {
			throw new AssertionError(e);
		}

		try {
			ds.storeExpressionEquivalence(expression3Id, expression1Id);
			ds.storeExpressionEquivalence(expression3Id, expression2Id);
			fail("A RelativeAlreadySetException should be thrown when setting an already set expression equivalence id.");
		} catch (RelativeAlreadySetException e) {
			// Everything is correct.
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		parent.add(conceptKingdomAnimalia);
		try {
			ds.storeExpressionParentsAndChildren(expression4Id, parent, child);
			ds.storeExpressionEquivalence(expression4Id, expression2Id);
			fail("A RelativeAlreadySetException should be thrown when setting a expression equivalence id to an expression that already have a relative.");
		} catch (RelativeAlreadySetException e) {
			// Everything is correct.
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet rs1 = stmt
					.executeQuery("SELECT equivalentid FROM expressions WHERE id = "
							+ expression1Id + ";");
			assertTrue("No expression returned.", rs1.next());
			expression1IdEquivalent = new ExpressionId(rs1.getLong(1));
			final ResultSet rs2 = stmt
					.executeQuery("SELECT equivalentid FROM expressions WHERE id = "
							+ expression2Id + ";");
			assertTrue("No expression returned.", rs2.next());
			expression2IdEquivalent = new ExpressionId(rs2.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
		assertTrue("The expression was not stored as equal to a concept.",
				expression1IdEquivalent.equals(conceptKingdomAnimalia));
		assertTrue(
				"The expression was not stored as equal as another expression.",
				expression2IdEquivalent.equals(expression1IdEquivalent));
		assertTrue(
				"The expression was not stored as equal as the initial concept.",
				expression2IdEquivalent.equals(conceptKingdomAnimalia));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#storeExpressionParentsAndChildren(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Set, java.util.Set)}
	 * .
	 */
	@Test
	public final void testStoreExpressionParentsAndChildren() {
		final String expression = "40";
		final String expressionParent = "41";
		final String expressionChild1 = "42";
		final String expressionChild2 = "43";
		final ExpressionId expressionId;
		final ExpressionId expressionParentId;
		final ExpressionId expressionChild1Id;
		final ExpressionId expressionChild2Id;
		final ExpressionId conceptDisease = new ExpressionId((long) 64572001);
		final ExpressionId conceptAcuteDisease = new ExpressionId(
				(long) 2704003);
		final Set<ExpressionId> parents = new HashSet<ExpressionId>();
		final Set<ExpressionId> children = new HashSet<ExpressionId>();
		final Set<ExpressionId> parentsTested;
		final Set<ExpressionId> childrenTested;
		final Set<ExpressionId> ancestors;
		final Set<ExpressionId> descendants;
		final Set<ExpressionId> ancestorsTested;
		final Set<ExpressionId> descendantsTested;

		try {
			ancestors = ds.getAncestors(conceptAcuteDisease, null);
			descendants = ds.getDescendants(conceptAcuteDisease, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		try {
			expressionId = ds.storeExpression(expression, null);
			expressionParentId = ds.storeExpression(expressionParent, null);
			expressionChild1Id = ds.storeExpression(expressionChild1, null);
			expressionChild2Id = ds.storeExpression(expressionChild2, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		parents.add(expressionParentId);
		parents.add(conceptDisease);
		children.add(expressionChild1Id);
		children.add(expressionChild2Id);
		children.add(conceptAcuteDisease);
		ancestors.addAll(parents);
		descendants.addAll(children);

		try {
			ds.storeExpressionParentsAndChildren(expressionId, parents,
					children);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException e) {
			throw new AssertionError(e);
		}

		try {
			parentsTested = ds.getParents(expressionId, null);
			childrenTested = ds.getChildren(expressionId, null);
			ancestorsTested = ds.getAncestors(expressionId, null);
			descendantsTested = ds.getDescendants(expressionId, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		assertTrue("The stored and retreieved parents are not the same.",
				parents.equals(parentsTested));
		assertTrue("The stored and retreieved children are not the same.",
				children.equals(childrenTested));
		assertTrue("The stored and retreieved ancestors are not the same.",
				ancestors.equals(ancestorsTested));
		assertTrue("The stored and retreieved descendants are not the same.",
				descendants.equals(descendantsTested));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getExpressionId(java.lang.String, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetExpressionIdWithoutDate() {
		final String expressionWithoutDate = "50";
		final ExpressionId expressionWithoutDateId;
		final ExpressionId expressionWithoutDateIdTested;

		try {
			ds.storeExpression(expressionWithoutDate, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet rs1 = stmt
					.executeQuery("SELECT id FROM expressions WHERE expression = '"
							+ expressionWithoutDate + "' AND endtime IS NULL;");
			rs1.next();
			expressionWithoutDateId = new ExpressionId(rs1.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithoutDateIdTested = ds.getExpressionId(
					expressionWithoutDate, null);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertTrue("The stored and returned ids for the expression "
				+ expressionWithoutDate + " is not the same.",
				expressionWithoutDateIdTested.equals(expressionWithoutDateId));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getExpressionId(java.lang.String, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetExpressionIdWithDate() {
		final String expressionWithDate = "60";
		final ExpressionId expressionWithDateId;
		final ExpressionId expressionWithDateIdTested;
		final ExpressionId expressionWithDateIdTestedBefore;
		final ExpressionId expressionWithDateIdTestedAfter;
		final Date insertTime = new GregorianCalendar(2110, 12, 03, 16, 14, 32)
				.getTime();
		final Date beforeInsertTime = new GregorianCalendar(2108, 02, 04, 12,
				34, 82).getTime();
		final Date afterInsertTime = new GregorianCalendar(2111, 03, 06, 02,
				34, 54).getTime();

		try {
			ds.storeExpression(expressionWithDate, insertTime);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet rs2 = stmt
					.executeQuery("SELECT id FROM expressions WHERE expression = '"
							+ expressionWithDate
							+ "' AND starttime = '"
							+ toSQL(insertTime) + "';");
			rs2.next();
			expressionWithDateId = new ExpressionId(rs2.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithDateIdTestedBefore = ds.getExpressionId(
					expressionWithDate, beforeInsertTime);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertNull("The database returned an id for the time "
				+ beforeInsertTime + ", which is before the insert time "
				+ insertTime + ".", expressionWithDateIdTestedBefore);

		try {
			expressionWithDateIdTested = ds.getExpressionId(expressionWithDate,
					insertTime);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertTrue("The stored and returned ids for the expression "
				+ expressionWithDate + "at the insert time, " + insertTime
				+ ", is not the same.",
				expressionWithDateIdTested.equals(expressionWithDateId));

		try {
			expressionWithDateIdTestedAfter = ds.getExpressionId(
					expressionWithDate, afterInsertTime);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}
		assertTrue("The stored and returned ids for the expression "
				+ expressionWithDate + "after, " + afterInsertTime
				+ " the insert time, " + insertTime + ", is not the same.",
				expressionWithDateIdTested
						.equals(expressionWithDateIdTestedAfter));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getExpression(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetExpressionWithoutDate() {
		final String expressionWithoutDate = "100";
		final ExpressionId expressionWithoutDateId;
		final String expressionWithoutDateTested;

		try {
			expressionWithoutDateId = ds.storeExpression(expressionWithoutDate,
					null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithoutDateTested = ds.getExpression(
					expressionWithoutDateId, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		assertTrue("The stored expression " + expressionWithoutDate
				+ "and the returned expression " + expressionWithoutDateTested
				+ "at the current time is not the same.",
				expressionWithoutDate.equals(expressionWithoutDateTested));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getExpression(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetExpressionWithDate() {
		final String expressionWithDate = "110";
		final ExpressionId expressionWithDateId;
		@SuppressWarnings("unused")
		final String expressionWithDateTestedBefore;
		final String expressionWithDateTestedAfter;

		final Date insertTimeBefore = new GregorianCalendar(2107, 03, 12, 02,
				56, 21).getTime();
		final Date insertTime = new GregorianCalendar(2109, 03, 24, 07, 16, 12)
				.getTime();
		final Date insertTimeAfter = new GregorianCalendar(2113, 04, 10, 12,
				44, 24).getTime();

		try {
			expressionWithDateId = ds.storeExpression(expressionWithDate,
					insertTime);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			expressionWithDateTestedBefore = ds.getExpression(
					expressionWithDateId, insertTimeBefore);
			fail("A NonExistingIdException should be thrown when asking for a expression at a time before the expression was insterded.");
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		} catch (NonExistingIdException e1) {
			// Everything is correct.
		}

		try {
			expressionWithDateTestedAfter = ds.getExpression(
					expressionWithDateId, insertTimeAfter);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}

		assertTrue("The stored expression " + expressionWithDate
				+ "and the returned expression "
				+ expressionWithDateTestedAfter + "at " + insertTimeAfter
				+ "is not the same.",
				expressionWithDate.equals(expressionWithDateTestedAfter));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getDescendants(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetDescendantsWithoutDate() {
		final ExpressionId conceptAdvancedDirectiveStatus = new ExpressionId(
				(long) 310301000);
		final ExpressionId conceptAdvanceDirectiveDiscussedWithPatient = new ExpressionId(
				(long) 310302007);
		final ExpressionId conceptAdvanceDirectiveDiscussedWithRelative = new ExpressionId(
				(long) 310303002);
		final ExpressionId conceptActiveAdvanceDirective_CopyWithinChart_ = new ExpressionId(
				(long) 310305009);
		final ExpressionId conceptActiveAdvanceDirective = new ExpressionId(
				(long) 425392003);
		final ExpressionId conceptActiveDurablePowerOfAttorneyForHealthcare = new ExpressionId(
				(long) 425393008);
		final ExpressionId conceptActiveHealthcareWill = new ExpressionId(
				(long) 425394002);
		final ExpressionId conceptActiveLivingWill = new ExpressionId(
				(long) 425395001);
		final ExpressionId conceptActiveAdvanceDirectiveWithVerificationByFamily = new ExpressionId(
				(long) 425396000);
		final ExpressionId conceptActiveAdvanceDirectiveWithVerificationByHealthcareProfessional = new ExpressionId(
				(long) 425397009);
		final Set<ExpressionId> conceptDescendantsNow = new HashSet<ExpressionId>();
		conceptDescendantsNow.add(conceptAdvanceDirectiveDiscussedWithPatient);
		conceptDescendantsNow.add(conceptAdvanceDirectiveDiscussedWithRelative);
		conceptDescendantsNow.add(conceptActiveAdvanceDirective);
		conceptDescendantsNow
				.add(conceptActiveDurablePowerOfAttorneyForHealthcare);
		conceptDescendantsNow.add(conceptActiveHealthcareWill);
		conceptDescendantsNow.add(conceptActiveLivingWill);
		conceptDescendantsNow
				.add(conceptActiveAdvanceDirectiveWithVerificationByFamily);
		conceptDescendantsNow
				.add(conceptActiveAdvanceDirectiveWithVerificationByHealthcareProfessional);
		conceptDescendantsNow
				.add(conceptActiveAdvanceDirective_CopyWithinChart_);
		final Set<ExpressionId> conceptDescendantsTestedNow;
		try {
			conceptDescendantsTestedNow = ds.getDescendants(
					conceptAdvancedDirectiveStatus, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved descendants for the concept 310301000|advanced directive status| at the current time are not correct.",
				conceptDescendantsNow.equals(conceptDescendantsTestedNow));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getDescendants(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetDescendantsWithDate() {
		final ExpressionId conceptAdvancedDirectiveStatus = new ExpressionId(
				(long) 310301000);
		final ExpressionId conceptAdvanceDirectiveDiscussedWithPatient = new ExpressionId(
				(long) 310302007);
		final ExpressionId conceptAdvanceDirectiveDiscussedWithRelative = new ExpressionId(
				(long) 310303002);
		final ExpressionId conceptAdvanceDirectiveSigned = new ExpressionId(
				(long) 310304008);
		final ExpressionId conceptActiveAdvanceDirective_CopyWithinChart_ = new ExpressionId(
				(long) 310305009);
		final Set<ExpressionId> conceptDescendantsBefore = new HashSet<ExpressionId>();
		conceptDescendantsBefore
				.add(conceptAdvanceDirectiveDiscussedWithPatient);
		conceptDescendantsBefore
				.add(conceptAdvanceDirectiveDiscussedWithRelative);
		conceptDescendantsBefore
				.add(conceptActiveAdvanceDirective_CopyWithinChart_);
		conceptDescendantsBefore.add(conceptAdvanceDirectiveSigned);
		final Set<ExpressionId> conceptDescendantsTestedBefore;
		final Date before = new GregorianCalendar(2002, 01, 31).getTime();
		try {
			conceptDescendantsTestedBefore = ds.getDescendants(
					conceptAdvancedDirectiveStatus, before);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved descendants for the concept 310301000|advanced directive status| at 2002-01-31 are not correct.",
				conceptDescendantsBefore.equals(conceptDescendantsTestedBefore));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getChildren(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetChildrenWithoutDate() {
		final ExpressionId conceptAcuteAllergicReaction = new ExpressionId(
				(long) 241929008);
		final ExpressionId conceptAnaphylaxis = new ExpressionId(
				(long) 39579001);
		final Set<ExpressionId> conceptChildrenNow = new HashSet<ExpressionId>();
		conceptChildrenNow.add(conceptAnaphylaxis);
		final Set<ExpressionId> conceptChildrenTestedNow;
		try {
			conceptChildrenTestedNow = ds.getChildren(
					conceptAcuteAllergicReaction, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved children for the concept 241929008|acute allergic reaction| at the current time are not correct.",
				conceptChildrenNow.equals(conceptChildrenTestedNow));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getChildren(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetChildrenWithDate() {
		final ExpressionId conceptAcuteAllergicReaction = new ExpressionId(
				(long) 241929008);
		final ExpressionId conceptAnaphylactoidReaction = new ExpressionId(
				(long) 35001004);
		final ExpressionId conceptAnaphylaxis = new ExpressionId(
				(long) 39579001);
		final ExpressionId conceptAngioedema = new ExpressionId((long) 41291007);
		final Set<ExpressionId> conceptChildrenBefore = new HashSet<ExpressionId>();
		conceptChildrenBefore.add(conceptAnaphylactoidReaction);
		conceptChildrenBefore.add(conceptAnaphylaxis);
		conceptChildrenBefore.add(conceptAngioedema);
		final Set<ExpressionId> conceptChildrenTestedBefore;
		final Date before = new GregorianCalendar(2002, 01, 31).getTime();
		try {
			conceptChildrenTestedBefore = ds.getChildren(
					conceptAcuteAllergicReaction, before);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved children for the concept 241929008|acute allergic reaction| at 2002-01-31 are not correct.",
				conceptChildrenBefore.equals(conceptChildrenTestedBefore));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getAncestors(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetAncestorsWithoutDate() {
		final ExpressionId conceptIsA = new ExpressionId((long) 116680003);
		final ExpressionId conceptLinkageConcept = new ExpressionId(
				(long) 106237007);
		final ExpressionId conceptSnomedCtConcept = new ExpressionId(
				(long) 138875005);
		final ExpressionId conceptAttribute = new ExpressionId((long) 246061005);
		final ExpressionId conceptSnomedCtModelComponent = new ExpressionId(
				new Long("900000000000441003").longValue());
		final Set<ExpressionId> conceptAncestorsNow = new HashSet<ExpressionId>();
		conceptAncestorsNow.add(conceptLinkageConcept);
		conceptAncestorsNow.add(conceptSnomedCtConcept);
		conceptAncestorsNow.add(conceptAttribute);
		conceptAncestorsNow.add(conceptSnomedCtModelComponent);

		final Set<ExpressionId> conceptAncestorsTestedNow;
		try {
			conceptAncestorsTestedNow = ds.getAncestors(conceptIsA, null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved ancestors for the concept 116680003|is a| at the current time are not correct.",
				conceptAncestorsNow.equals(conceptAncestorsTestedNow));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getAncestors(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetAncestorsWithDate() {
		final ExpressionId conceptIsA = new ExpressionId((long) 116680003);
		final ExpressionId conceptLinkageConcept = new ExpressionId(
				(long) 106237007);
		final ExpressionId conceptSnomedCtConcept = new ExpressionId(
				(long) 138875005);
		final ExpressionId conceptAttribute = new ExpressionId((long) 246061005);
		final Set<ExpressionId> conceptAncestorsBefore = new HashSet<ExpressionId>();
		conceptAncestorsBefore.add(conceptLinkageConcept);
		conceptAncestorsBefore.add(conceptSnomedCtConcept);
		conceptAncestorsBefore.add(conceptAttribute);
		final Set<ExpressionId> conceptAncestorsTestedBefore;
		final Date before = new GregorianCalendar(2002, 01, 31).getTime();
		try {
			conceptAncestorsTestedBefore = ds.getAncestors(conceptIsA, before);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved ancestors for the concept 116680003|is a|  at 2002-01-31 are not correct.",
				conceptAncestorsBefore.equals(conceptAncestorsTestedBefore));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getParents(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetParentsWithoutDate() {
		final ExpressionId conceptCongenitalPneumonia = new ExpressionId(
				(long) 78895009);
		final ExpressionId conceptPneumonia = new ExpressionId((long) 233604007);
		final ExpressionId conceptCongenitalDisease = new ExpressionId(
				(long) 66091009);
		final Set<ExpressionId> conceptParentsNow = new HashSet<ExpressionId>();
		conceptParentsNow.add(conceptPneumonia);
		conceptParentsNow.add(conceptCongenitalDisease);
		final Set<ExpressionId> conceptParentsTestedNow;
		try {
			conceptParentsTestedNow = ds.getParents(conceptCongenitalPneumonia,
					null);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved parents for the concept 78895009|congenital pneumonia| at the current time are not correct.",
				conceptParentsNow.equals(conceptParentsTestedNow));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getParents(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetParentsWithDate() {
		final ExpressionId conceptCongenitalPneumonia = new ExpressionId(
				(long) 78895009);
		final ExpressionId conceptCongenitalDisease = new ExpressionId(
				(long) 66091009);
		final ExpressionId conceptNeonatalPneumonia = new ExpressionId(
				(long) 233619008);
		final Set<ExpressionId> conceptParentsBefore = new HashSet<ExpressionId>();
		conceptParentsBefore.add(conceptCongenitalDisease);
		conceptParentsBefore.add(conceptNeonatalPneumonia);
		final Set<ExpressionId> conceptParentsTestedBefore;
		final Date before = new GregorianCalendar(2005, 01, 31).getTime();
		try {
			conceptParentsTestedBefore = ds.getParents(
					conceptCongenitalPneumonia, before);
		} catch (DataStoreException | NonExistingIdException e) {
			throw new AssertionError(e);
		}
		assertTrue(
				"The retreieved parents for the concept 78895009|congenital pneumonia| at 2002-01-31 are not correct.",
				conceptParentsBefore.equals(conceptParentsTestedBefore));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getAllExpressions(java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetAllExpressionsWithoutDate() {
		try {
			DataStoreTest.tearDownAfterClass();
		} catch (Exception e) {
			throw new AssertionError(e);
		}

		final String expressionString1 = "71";
		final String expressionString2 = "72";
		final String expressionString3 = "73";
		final String expressionString4 = "74";
		final String expressionString5 = "75";
		final Set<Expression> expressions = new HashSet<Expression>();
		final Set<Expression> expressionsTested;

		try {
			ds.storeExpression(expressionString1, null);
			ds.storeExpression(expressionString2, null);
			ds.storeExpression(expressionString3, null);
			ds.storeExpression(expressionString4, null);
			ds.storeExpression(expressionString5, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet expressionsRs = stmt
					.executeQuery("SELECT id, expression FROM expressions WHERE endtime IS NULL;");
			while (expressionsRs.next()) {
				expressions.add(new Expression(new ExpressionId(expressionsRs
						.getLong(1)), expressionsRs.getString(2)));
			}

		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionsTested = ds.getAllExpressions(null);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		assertTrue("The stored and retrieved expressions are not the same.",
				expressions.equals(expressionsTested));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#getAllExpressions(java.util.Date)}
	 * .
	 */
	@Test
	public final void testGetAllExpressionsWithDate() {
		try {
			DataStoreTest.tearDownAfterClass();
		} catch (Exception e) {
			throw new AssertionError(e);
		}

		final Date insertTime1 = new GregorianCalendar(2101, 02, 21).getTime();
		final Date insertTimeBetween = new GregorianCalendar(2103, 11, 10)
				.getTime();
		final Date insertTime2 = new GregorianCalendar(2106, 01, 12).getTime();

		final String expressionString1 = "121";
		final String expressionString2 = "122";
		final String expressionString3 = "123";
		final String expressionString4 = "124";
		final String expressionString5 = "125";
		final Set<Expression> expressions = new HashSet<Expression>();
		final Set<Expression> expressionsTested;

		try {
			ds.storeExpression(expressionString1, insertTime1);
			ds.storeExpression(expressionString2, insertTime1);
			ds.storeExpression(expressionString3, insertTime1);
			ds.storeExpression(expressionString4, insertTime2);
			ds.storeExpression(expressionString5, insertTime2);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet expressionsRs = stmt
					.executeQuery("SELECT id, expression FROM expressions WHERE starttime <= '2103-11-10' AND ('2103-11-10' < endtime OR endtime IS NULL);");
			while (expressionsRs.next()) {
				expressions.add(new Expression(new ExpressionId(expressionsRs
						.getLong(1)), expressionsRs.getString(2)));
			}

		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionsTested = ds.getAllExpressions(insertTimeBetween);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		assertTrue("The stored and retrieved expressions are not the same.",
				expressions.equals(expressionsTested));
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isSubsumingNotEquivalent(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsSubsumingNotEquivalentWithoutDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		try {
			expressionEqualToBodyStructure = ds.storeExpression("123037004",
					null);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("71388002", null);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"128927009", null);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 128927009 | procedure by method |.",
					ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptProcedureByMethod, null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept 71388002 | procedure |.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							conceptProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should not subsume the concept  123037004 | body structure |.",
					!ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptBodyStructure, null));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  71388002 | procedure |.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							conceptProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should not be unequivalent to the concept 71388002 | procedure |.",
					!ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptProcedure, null));

			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, null));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, null));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not be unequivalent to the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, null));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method |.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure |.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, null));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should not be unequivalent to the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(conceptProcedure,
							expressionEqualToProcedure, null));

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
	public final void testIsSubsumingNotEquivalentWithDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		final Date insertTimeBefore = new GregorianCalendar(2102, 03, 04, 04,
				41, 22).getTime();
		final Date insertTime = new GregorianCalendar(2103, 04, 05, 05, 21, 25)
				.getTime();
		final Date insertTimeAfter = new GregorianCalendar(2105, 02, 01, 07,
				22, 12).getTime();

		try {
			expressionEqualToBodyStructure = ds.storeExpression("0123037004",
					insertTime);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("071388002",
					insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"0128927009", insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptProcedureByMethod, insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							conceptProcedure, insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should not subsume the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							conceptProcedure, insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should not be unequivalent to the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptProcedure,
							conceptProcedure, insertTimeBefore));

			// Before insert time.
			assertTrue(
					"The expressionEqualToProcedure should not subsume the concept expressionEqualToProcedureByMethod at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod,
							insertTimeBefore));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not be unequivalent to the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeBefore));

			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));

			// Insert time.
			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod at the time 2103-04-05 05:21:25.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTime));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTime));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not be unequivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTime));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method | at the time 2103-04-05 05:21:25.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTime));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTime));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTime));

			// After insert time.
			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod at the time 2105-02-01 07:22:12.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(
							expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not be unequivalent to the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeAfter));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method | at the time 2105-02-01 07:22:12.",
					ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsumingNotEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));

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
	public final void testIsEquivalentWithoutDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		try {
			expressionEqualToBodyStructure = ds.storeExpression("00123037004",
					null);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("0071388002", null);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"00128927009", null);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should not be equivalent to the concept 128927009 | procedure by method |.",
					!ds.isEquivalent(conceptProcedure,
							conceptProcedureByMethod, null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept 71388002 | procedure |.",
					!ds.isEquivalent(conceptProcedureByMethod,
							conceptProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should not be equivalent to the concept  123037004 | body structure |.",
					!ds.isEquivalent(conceptProcedure, conceptBodyStructure,
							null));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  71388002 | procedure |.",
					!ds.isEquivalent(conceptBodyStructure, conceptProcedure,
							null));
			assertTrue(
					"The concept 71388002 | procedure | should be equivalent to the concept 71388002 | procedure |.",
					ds.isEquivalent(conceptProcedure, conceptProcedure, null));

			assertTrue(
					"The expressionEqualToProcedure should not be equivalent to the concept expressionEqualToProcedureByMethod.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, null));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not be equivalent to the concept expressionEqualToProcedure.",
					!ds.isEquivalent(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept expressionEqualToBodyStructure.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, null));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not be equivalent to the concept expressionEqualToProcedure.",
					!ds.isEquivalent(expressionEqualToBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should be equivalent to the concept expressionEqualToProcedure.",
					ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, null));

			assertTrue(
					"The concept expressionEqualToProcedure should be equivalent to the concept expressionEqualToProcedure.",
					ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, null));

			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept 128927009 | procedure by method |.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept expressionEqualToProcedure.",
					!ds.isSubsumingNotEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept  123037004 | body structure |.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, null));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  expressionEqualToProcedure.",
					!ds.isEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should be equivalent to the concept expressionEqualToProcedure.",
					ds.isEquivalent(conceptProcedure,
							expressionEqualToProcedure, null));

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
	public final void testIsEquivalentWithDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		final Date insertTimeBefore = new GregorianCalendar(2102, 03, 04, 04,
				41, 22).getTime();
		final Date insertTime = new GregorianCalendar(2103, 04, 05, 05, 21, 25)
				.getTime();
		final Date insertTimeAfter = new GregorianCalendar(2105, 02, 01, 07,
				22, 12).getTime();

		try {
			expressionEqualToBodyStructure = ds.storeExpression("000123037004",
					insertTime);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("00071388002",
					insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"000128927009", insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should not be equivalent to the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptProcedure,
							conceptProcedureByMethod, insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptProcedureByMethod,
							conceptProcedure, insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should not be equivalent to the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptProcedure, conceptBodyStructure,
							insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptBodyStructure, conceptProcedure,
							insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should be equivalent to the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					ds.isEquivalent(conceptProcedure, conceptProcedure,
							insertTimeBefore));

			// Before insert time.
			assertTrue(
					"The expressionEqualToProcedure should not be equivalent to the concept expressionEqualToProcedureByMethod at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod,
							insertTimeBefore));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not be equivalent to the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept expressionEqualToBodyStructure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not be equivalent to the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeBefore));

			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));

			// Insert time.
			assertTrue(
					"The expressionEqualToProcedure should not be equivalent to the concept expressionEqualToProcedureByMethod at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTime));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept expressionEqualToBodyStructure at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTime));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTime));

			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to concept 128927009 | procedure by method | at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTime));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept  123037004 | body structure | at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTime));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept 71388002 | procedure | should be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					ds.isEquivalent(conceptProcedure,
							expressionEqualToProcedure, insertTime));

			// After insert time.
			assertTrue(
					"The expressionEqualToProcedure should not be equivalent to the concept expressionEqualToProcedureByMethod at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not be equivalent to the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept expressionEqualToBodyStructure at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not be equivalent to the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should be equivalent to the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					ds.isEquivalent(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeAfter));

			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept 128927009 | procedure by method | at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The concept 128927009 | procedure by method | should not be equivalent to the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not be equivalent to the concept  123037004 | body structure | at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept 123037004 | body structure | should not be equivalent to the concept  expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isEquivalent(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept 71388002 | procedure | should be equivalent to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					ds.isEquivalent(conceptProcedure,
							expressionEqualToProcedure, insertTimeAfter));
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isExistingId(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsExistingIdWithoutDate() {
		final String expressionString2 = "80";
		final ExpressionId expressionId1 = new ExpressionId((long) 138875005);
		final ExpressionId expressionId2;
		final ExpressionId expressionId3;
		final boolean expressionId1ExistsTested;
		final boolean expressionId2ExistsTested;
		final boolean expressionId3ExistsTested;

		try {
			ds.storeExpression(expressionString2, null);
			expressionId2 = ds.getExpressionId(expressionString2, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet expressionsRs = stmt
					.executeQuery("SELECT MIN(id) - 1 FROM expressions");
			expressionsRs.next();
			expressionId3 = new ExpressionId(expressionsRs.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionId1ExistsTested = ds.isExistingId(expressionId1, null);
			expressionId2ExistsTested = ds.isExistingId(expressionId2, null);
			expressionId3ExistsTested = ds.isExistingId(expressionId3, null);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		assertTrue(
				"The method says that the concept 138875005 | SNOMED CT Concept | doesn't exist in the data store at the current time, but it is the root concept in SNOMED CT.",
				expressionId1ExistsTested == true);

		assertTrue(
				"The method says that the expression with id "
						+ expressionId2.toString()
						+ " doesn't exist in the data store at the current time , but it is stored there.",
				expressionId2ExistsTested == true);

		assertTrue(
				"The method says that the expression with id "
						+ expressionId3.toString()
						+ " exist in the data store at the current time , but it is not stored there.",
				expressionId3ExistsTested == false);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isExistingId(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsExistingIdWithDate() {
		final String expressionString2 = "90";
		final ExpressionId expressionId1 = new ExpressionId((long) 138875005);
		final ExpressionId expressionId2;
		final ExpressionId expressionId3;
		final Date insertTimeBefore = new GregorianCalendar(2109, 02, 23, 03,
				01, 22).getTime();
		final Date insertTime = new GregorianCalendar(2111, 03, 04, 23, 15, 21)
				.getTime();
		final Date insertTimeAfter = new GregorianCalendar(2112, 02, 11, 12,
				03, 14).getTime();
		final boolean expressionId1ExistsTestedBefore;
		final boolean expressionId1ExistsTestedNow;
		final boolean expressionId1ExistsTestedAfter;
		final boolean expressionId2ExistsTestedBefore;
		final boolean expressionId2ExistsTestedNow;
		final boolean expressionId2ExistsTestedAfter;
		final boolean expressionId3ExistsTestedBefore;
		final boolean expressionId3ExistsTestedNow;
		final boolean expressionId3ExistsTestedAfter;

		try {
			ds.storeExpression(expressionString2, insertTime);
			expressionId2 = ds.getExpressionId(expressionString2, null);
		} catch (DataStoreException | ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			final ResultSet expressionsRs = stmt
					.executeQuery("SELECT MIN(id) - 1 FROM expressions");
			expressionsRs.next();
			expressionId3 = new ExpressionId(expressionsRs.getLong(1));
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		try {
			expressionId1ExistsTestedBefore = ds.isExistingId(expressionId1,
					insertTimeBefore);
			expressionId1ExistsTestedNow = ds.isExistingId(expressionId1, null);
			expressionId1ExistsTestedAfter = ds.isExistingId(expressionId1,
					insertTimeAfter);
			expressionId2ExistsTestedBefore = ds.isExistingId(expressionId2,
					insertTimeBefore);
			expressionId2ExistsTestedNow = ds.isExistingId(expressionId2, null);
			expressionId2ExistsTestedAfter = ds.isExistingId(expressionId2,
					insertTimeAfter);
			expressionId3ExistsTestedBefore = ds.isExistingId(expressionId3,
					insertTimeBefore);
			expressionId3ExistsTestedNow = ds.isExistingId(expressionId3, null);
			expressionId3ExistsTestedAfter = ds.isExistingId(expressionId3,
					insertTimeAfter);
		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

		assertTrue(
				"The method says that the concept 138875005 | SNOMED CT Concept | doesn't exist in the data store at 2009-02-23, but it is the root concept in SNOMED CT.",
				expressionId1ExistsTestedBefore == true);
		assertTrue(
				"The method says that the concept 138875005 | SNOMED CT Concept | doesn't exist in the data store at 2111-03-04, but it is the root concept in SNOMED CT.",
				expressionId1ExistsTestedNow == true);
		assertTrue(
				"The method says that the concept 138875005 | SNOMED CT Concept | doesn't exist in the data store at 2012-02-11, but it is the root concept in SNOMED CT.",
				expressionId1ExistsTestedAfter == true);

		assertTrue(
				"The method says that the expression with id "
						+ expressionId2.toString()
						+ " does exist in the data store at 2109-02-23, but it is not stored there yet .",
				expressionId2ExistsTestedBefore == false);
		assertTrue(
				"The method says that the expression with id "
						+ expressionId2.toString()
						+ " doesn't exist in the data store at 2111-03-04, but it is stored there.",
				expressionId2ExistsTestedNow == true);
		assertTrue(
				"The method says that the expression with id "
						+ expressionId2.toString()
						+ " doesn't exist in the data store at 2112-02-11, but it is stored there.",
				expressionId2ExistsTestedAfter == true);

		assertTrue(
				"The method says that the expression with id "
						+ expressionId3.toString()
						+ " exist in the data store at 2109-02-23, but it is not stored there.",
				expressionId3ExistsTestedBefore == false);
		assertTrue(
				"The method says that the expression with id "
						+ expressionId3.toString()
						+ " exist in the data store at 2111-03-04, but it is not stored there.",
				expressionId3ExistsTestedNow == false);
		assertTrue(
				"The method says that the expression with id "
						+ expressionId3.toString()
						+ " exist in the data store at 2112-02-11, but it is not stored there.",
				expressionId3ExistsTestedAfter == false);
	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isSubsuming(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsSubsumingWithoutDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		try {
			expressionEqualToBodyStructure = ds.storeExpression(
					"0000123037004", null);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("000071388002",
					null);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"0000128927009", null);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 128927009 | procedure by method |.",
					ds.isSubsuming(conceptProcedure, conceptProcedureByMethod,
							null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept 71388002 | procedure |.",
					!ds.isSubsuming(conceptProcedureByMethod, conceptProcedure,
							null));
			assertTrue(
					"The concept 71388002 | procedure | should not subsume the concept  123037004 | body structure |.",
					!ds.isSubsuming(conceptProcedure, conceptBodyStructure,
							null));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  71388002 | procedure |.",
					!ds.isSubsuming(conceptBodyStructure, conceptProcedure,
							null));
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 71388002 | procedure |.",
					ds.isSubsuming(conceptProcedure, conceptProcedure, null));

			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, null));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsuming(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToBodyStructure, null));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsuming(expressionEqualToBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept expressionEqualToProcedure.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedure, null));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method |.",
					ds.isSubsuming(expressionEqualToProcedure,
							conceptProcedureByMethod, null));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure.",
					!ds.isSubsuming(conceptProcedureByMethod,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure |.",
					!ds.isSubsuming(expressionEqualToProcedure,
							conceptBodyStructure, null));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure.",
					!ds.isSubsuming(conceptBodyStructure,
							expressionEqualToProcedure, null));
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept expressionEqualToProcedure.",
					ds.isSubsuming(conceptProcedure,
							expressionEqualToProcedure, null));

		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

	}

	/**
	 * Test method for
	 * {@link se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore#isSubsuming(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, java.util.Date)}
	 * .
	 */
	@Test
	public final void testIsSubsumingWithDate() {
		final ExpressionId conceptBodyStructure = new ExpressionId(
				(long) 123037004);
		final ExpressionId conceptProcedure = new ExpressionId((long) 71388002);
		final ExpressionId conceptProcedureByMethod = new ExpressionId(
				(long) 128927009);
		final ExpressionId expressionEqualToBodyStructure;
		final ExpressionId expressionEqualToProcedure;
		final ExpressionId expressionEqualToProcedureByMethod;

		final Date insertTimeBefore = new GregorianCalendar(2102, 03, 04, 04,
				41, 22).getTime();
		final Date insertTime = new GregorianCalendar(2103, 04, 05, 05, 21, 25)
				.getTime();
		final Date insertTimeAfter = new GregorianCalendar(2105, 02, 01, 07,
				22, 12).getTime();

		try {
			expressionEqualToBodyStructure = ds.storeExpression(
					"00000123037004", insertTime);
			ds.storeExpressionEquivalence(expressionEqualToBodyStructure,
					conceptBodyStructure);
			expressionEqualToProcedure = ds.storeExpression("0000071388002",
					insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedure,
					conceptProcedure);
			expressionEqualToProcedureByMethod = ds.storeExpression(
					"00000128927009", insertTime);
			ds.storeExpressionEquivalence(expressionEqualToProcedureByMethod,
					conceptProcedureByMethod);
		} catch (DataStoreException | NonExistingIdException
				| RelativeAlreadySetException
				| ExpressionAlreadyExistsException e) {
			throw new AssertionError(e);
		}

		try {
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					ds.isSubsuming(conceptProcedure, conceptProcedureByMethod,
							insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(conceptProcedureByMethod, conceptProcedure,
							insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should not subsume the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(conceptProcedure, conceptBodyStructure,
							insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  71388002 | procedure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(conceptBodyStructure, conceptProcedure,
							insertTimeBefore));
			assertTrue(
					"The concept 71388002 | procedure | should subsume the concept 71388002 | procedure | at the time 2102-03-04 04:41:22.",
					ds.isSubsuming(conceptProcedure, conceptProcedure,
							insertTimeBefore));

			// Before insert time.
			assertTrue(
					"The expressionEqualToProcedure should not subsume the concept expressionEqualToProcedureByMethod at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod,
							insertTimeBefore));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeBefore));

			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept 128927009 | procedure by method | at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeBefore));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeBefore));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeBefore));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2102-03-04 04:41:22.",
					!ds.isSubsuming(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeBefore));

			// Insert time.
			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod at the time 2103-04-05 05:21:25.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTime));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTime));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should subsume to the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTime));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method | at the time 2103-04-05 05:21:25.",
					ds.isSubsuming(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTime));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTime));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(expressionEqualToProcedure,
							conceptBodyStructure, insertTime));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2103-04-05 05:21:25.",
					!ds.isSubsuming(conceptBodyStructure,
							expressionEqualToProcedure, insertTime));

			// After insert time.
			assertTrue(
					"The expressionEqualToProcedure should subsume the concept expressionEqualToProcedureByMethod at the time 2105-02-01 07:22:12.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The expressionEqualToProcedureByMethod should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(expressionEqualToProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept expressionEqualToBodyStructure at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToBodyStructure should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(expressionEqualToBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					ds.isSubsuming(expressionEqualToProcedure,
							expressionEqualToProcedure, insertTimeAfter));

			assertTrue(
					"The concept expressionEqualToProcedure should subsume the concept 128927009 | procedure by method | at the time 2105-02-01 07:22:12.",
					ds.isSubsuming(expressionEqualToProcedure,
							conceptProcedureByMethod, insertTimeAfter));
			assertTrue(
					"The concept 128927009 | procedure by method | should not subsume the concept expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(conceptProcedureByMethod,
							expressionEqualToProcedure, insertTimeAfter));
			assertTrue(
					"The concept expressionEqualToProcedure should not subsume the concept  123037004 | body structure | at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(expressionEqualToProcedure,
							conceptBodyStructure, insertTimeAfter));
			assertTrue(
					"The concept 123037004 | body structure | should not subsume the concept  expressionEqualToProcedure at the time 2105-02-01 07:22:12.",
					!ds.isSubsuming(conceptBodyStructure,
							expressionEqualToProcedure, insertTimeAfter));

		} catch (DataStoreException e) {
			throw new AssertionError(e);
		}

	}

	/**
	 * Convert a <code>Date</code> to a <code>String</code> suitable to use in
	 * SQL queries.
	 * 
	 * @param date
	 *            The date to convert.
	 * @return The converted date.
	 */
	@SuppressWarnings("deprecation")
	private String toSQL(Date date) {
		return (1900 + date.getYear()) + "-" + (date.getMonth() + 1) + "-"
				+ date.getDate() + " " + date.getHours() + ":"
				+ date.getMinutes() + ":" + date.getSeconds();
	}

}
