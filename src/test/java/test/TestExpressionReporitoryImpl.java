package test;

import static org.junit.Assert.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import se.liu.imt.mi.snomedct.expression.tools.ExpressionSyntaxError;
import se.liu.imt.mi.snomedct.expressionrepository.ExpressionRepositoryImpl;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStoreService;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

public class TestExpressionReporitoryImpl {

	private static final Logger log = Logger.getLogger(TestExpressionReporitoryImpl.class);

	private static ExpressionRepository repo = null;

	private static Date date = null;

	/**
	 * Setup environment before any test, including storing current time and creating a
	 * <code>ExpressionRepository</code> instance.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void oneTimeSetUp() throws Exception {
		date = new Date();

		repo = new ExpressionRepositoryImpl();

	}

	/**
	 * Tear down test including resetting the database to the state before the test.
	 * 
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Configuration config = null;
		config = new XMLConfiguration("config.xml");

		String url = config.getString("database.url");
		String username = config.getString("database.username");
		String password = config.getString("database.password");

		DataStoreService dss = new DataStoreService(url, username, password);
		dss.restoreDataStore(date);
		log.debug("Restored to " + (new SimpleDateFormat()).format(date));
	}

	@Test
	public final void testGetSCTQueryResult() throws Exception {
		// log.debug("testGetSCTQueryResult()");
		// Collection<ExpressionId> result = repo
		// .getSCTQueryResult("Descendants(5913000|Fracture of neck of femur (disorder)|)");
		// log.debug("result = " + result.toString());
		// log.debug("result size = " + result.size());
		// assertEquals(36, result.size()); // depends on SNOMED CT release, might change
	}

	@Test
	public final void testGetExpressionID() throws ExpressionSyntaxError, NonExistingIdException {
		log.debug("testGetExpressionID()");
		ExpressionId id = repo
				.getExpressionID("125605004 | fracture of bone | : 363698007 | finding site | = 71341001 | bone structure of femur |");
		log.debug("expression id = " + id.toString());
		assertNotNull(id);
	}

	@Test
	public final void testGetExpression() throws ExpressionSyntaxError, NonExistingIdException {
		// log.debug("testGetExpression()");
		// ExpressionId id = repo
		// .getExpressionID("125605004 | fracture of bone | : 363698007 | finding site | = 71341001 | bone structure of femur |");
		// String expression = repo.getExpression(id);
		// log.debug("expression = " + expression);
		// assertTrue(expression.equals("125605004:363698007=71341001"));
	}

	@Test
	public final void testGetDecendants() throws NonExistingIdException, DataStoreException {
		log.debug("testGetDecendants()");
		ExpressionId id = new ExpressionId((long) 5913000);
		Collection<ExpressionId> result = repo.getDecendants(id);
		log.debug("result = " + result.toString());
		log.debug("result size = " + result.size());
		assertEquals(34, result.size());
	}

	@Test
	public final void testGetChildren() throws ExpressionSyntaxError, NonExistingIdException, DataStoreException {
		log.debug("testGetChildren()");
		ExpressionId id = repo
				.getExpressionID("125605004 | fracture of bone | : 363698007 | finding site | = 71341001 | bone structure of femur |");
		Collection<ExpressionId> result = repo.getChildren(id);
		log.debug("result = " + result.toString());
		log.debug("result size = " + result.size());
		assertEquals(1, result.size());
	}

	@Test
	public final void testGetAncestors() {
		// fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGetParents() {
		// fail("Not yet implemented"); // TODO
	}

}
