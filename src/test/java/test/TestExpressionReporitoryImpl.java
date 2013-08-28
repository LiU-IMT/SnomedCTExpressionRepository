package test;

import static org.junit.Assert.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import se.liu.imt.mi.snomedct.expressionrepository.ExpressionRepositoryImpl;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionSyntaxError;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStoreService;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

public class TestExpressionReporitoryImpl {

	private static final Logger log = Logger
			.getLogger(ExpressionRepositoryImpl.class);

	private static ExpressionRepository repo = null;

	/**
	 * Setup environment before any test, including resetting database to 2012-08-01
	 * and creating a <code>ExpressionRepository</code> instance.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void oneTimeSetUp() throws Exception {
		Configuration config = null;
		config = new XMLConfiguration("config.xml");

		String url = config.getString("database.url");
		String username = config.getString("database.username");
		String password = config.getString("database.password");

		String date = "2012-08-01";

		DataStoreService dss = new DataStoreService(url, username, password);
		DateFormat formatter = new SimpleDateFormat("YY-MM-DD");
		dss.restoreDataStore(formatter.parse(date));
		log.debug("Restored to " + date);

		repo = new ExpressionRepositoryImpl();

	}

	@Test
	public final void testGetSCTQueryResult() throws Exception {
		log.debug("testGetSCTQueryResult()");
		Collection<ExpressionId> result = repo
				.getSCTQueryResult("Descendants(5913000|Fracture of neck of femur (disorder)|)");
		log.debug("result = " + result.toString());
		log.debug("result size = " + result.size());
		assertEquals(36, result.size()); // depends on SNOMED CT release, might change
	}

	@Test
	public final void testGetExpressionID() throws ExpressionSyntaxError,
			NonExistingIdException {
		log.debug("testGetExpressionID()");
		ExpressionId id = repo
				.getExpressionID("125605004 | fracture of bone | : 363698007 | finding site | = 71341001 | bone structure of femur |");
		log.debug("expression id = " +  id.toString());
		assertNotNull(id);
	}

	@Test
	public final void testGetExpression() throws ExpressionSyntaxError, NonExistingIdException {
//		log.debug("testGetExpression()");
//		ExpressionId id = repo
//				.getExpressionID("125605004 | fracture of bone | : 363698007 | finding site | = 71341001 | bone structure of femur |");
//		String expression = repo.getExpression(id);
//		log.debug("expression = " + expression);
//		assertTrue(expression.equals("125605004:363698007=71341001"));
	}

	@Test
	public final void testGetDecendants() throws NonExistingIdException, DataStoreException {
		log.debug("testGetDecendants()");
		ExpressionId id = new ExpressionId((long) 5913000);
		Collection<ExpressionId> result = repo.getDecendants(id);
		log.debug("result = " + result.toString());
		log.debug("result size = " + result.size());
		assertEquals(36, result.size());
	}

	@Test
	public final void testGetChildren() {
//		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGetAncestors() {
//		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGetParents() {
//		fail("Not yet implemented"); // TODO
	}

}
