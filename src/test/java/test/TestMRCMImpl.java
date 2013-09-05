/**
 * 
 */
package test;


import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import se.liu.imt.mi.snomedct.expressionrepository.ExpressionRepositoryImpl;
import se.liu.imt.mi.snomedct.expressionrepository.MRCMImpl;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository;
import se.liu.imt.mi.snomedct.expressionrepository.api.MRCM;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * @author daniel
 *
 */
public class TestMRCMImpl {
	
	private MRCM mrcm;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
//		ExpressionRepositoryImpl repo = new ExpressionRepositoryImpl();
		mrcm = new MRCMImpl(null);
		mrcm.loadMRCM("src/test/resources/refset_MRCM_preview-20130327.txt");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testValidate() throws Exception {
		assertTrue(mrcm.validate(new ExpressionId(34000006L), new ExpressionId(363698007L), new ExpressionId(51289009L))); // Crohns disease:finding site=Digestive tract structure
	}

}
