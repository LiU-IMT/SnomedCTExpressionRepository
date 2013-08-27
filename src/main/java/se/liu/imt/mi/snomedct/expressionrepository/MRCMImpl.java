/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import se.liu.imt.mi.snomedct.expression.SCTExpressionLexer;
import se.liu.imt.mi.snomedct.expression.SCTExpressionParser;
import se.liu.imt.mi.snomedct.expressionrepository.api.MRCM;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * An implementation of the MRCM as it exists at the time being (Aug 2013). This
 * implementation assumes that domains and ranges can only be expressed as a
 * union of either single concepts, descendants of a single concepts, or
 * descendants of a single concept including itself. No other expressivity is allowd.
 * 
 * @author daniel
 * 
 */
public class MRCMImpl implements MRCM {
	
	private List<MRCMEntry> mrcmList = null;
	
	private ExpressionRepositoryImpl repo = null;
	
	private class MRCMEntry {
		ExpressionId relationship;
		MRCMRestriction domain;
		MRCMRestriction range;
		
		public ExpressionId getRelationship() {
			return relationship;
		}

		public MRCMRestriction getDomain() {
			return domain;
		}

		public MRCMRestriction getRange() {
			return range;
		}

		public MRCMEntry(ExpressionId relationship, MRCMRestriction domain,
				MRCMRestriction range) {
			super();
			this.relationship = relationship;
			this.domain = domain;
			this.range = range;
		}
	}

	private class MRCMRestriction {
		List<ExpressionId> descendantsList; // list of allowed descendants not
											// including self
		List<ExpressionId> descendantsAndSelfList; // list of allowed
													// descendants including
													// self
		List<ExpressionId> singlesList; // list of allowed concepts where
										// descendants are not allowed

		MRCMRestriction() {
			descendantsList = new ArrayList<ExpressionId>();
			descendantsAndSelfList = new ArrayList<ExpressionId>();
			singlesList = new ArrayList<ExpressionId>();
		}

		void addDescendants(ExpressionId desc) {
			descendantsList.add(desc);
		}

		void addDescendantsAndSelf(ExpressionId desc) {
			descendantsAndSelfList.add(desc);
		}

		void addSingles(ExpressionId desc) {
			singlesList.add(desc);
		}

		List<ExpressionId> getDescendants() {
			return descendantsList;
		}

		List<ExpressionId> getDescendantsAndSelf() {
			return descendantsAndSelfList;
		}

		List<ExpressionId> getSingles() {
			return singlesList;
		}
	}

	static int ACTIVE = 2;
	static int RELATIONSHIP = 5;
	static int DOMAIN = 6;
	static int RANGE = 7;

	private static final Logger log = Logger.getLogger(MRCMImpl.class);
	
	

	public MRCMImpl(ExpressionRepositoryImpl repo) {
		super();
		this.repo = repo;
		mrcmList = new ArrayList<MRCMEntry>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.MRCM#loadMRCM(java.lang
	 * .String)
	 */
	@Override
	public void loadMRCM(String fileName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));

		String strLine;

		try {
			while ((strLine = reader.readLine()) != null) {
				String[] items = strLine.split("\t");
				if (items[ACTIVE].equals("1")) {
					log.debug("Relationship " + items[RELATIONSHIP]);
					log.debug("Domain " + items[DOMAIN]);
					log.debug("Range " + items[RANGE]);

					try {
						ExpressionId relationship = new ExpressionId(
								Long.decode(items[RELATIONSHIP]));
						MRCMRestriction domain = parseMRCMEntry(items[DOMAIN]);
						MRCMRestriction range = parseMRCMEntry(items[RANGE]);
						
						MRCMEntry entry = new MRCMEntry(relationship, domain, range);
						mrcmList.add(entry);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		reader.close();
	}

	private MRCMRestriction parseMRCMEntry(String s)
			throws Exception {

		CharStream input = new ANTLRStringStream(s);
		SCTExpressionLexer lexer = new SCTExpressionLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		SCTExpressionParser parser = new SCTExpressionParser(tokens);
		SCTExpressionParser.query_return result = null;
		try {
			result = parser.query();
		} catch (RecognitionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Tree ast = (Tree) result.getTree();
		
		log.debug(SCTExpressionParser.tokenNames[ast.getType()]);
		
//		TestSCTExpressionParser.printTree(ast, 0);

		switch (ast.getType()) {
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND: {
			MRCMRestriction r = new MRCMRestriction();
			Long id = Long.decode(ast.getChild(0).getChild(0).getText());
			r.addSingles(new ExpressionId(id));
			return r;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.UNION: {
			MRCMRestriction r = new MRCMRestriction();
			for (int i = 0; i < ast.getChildCount(); i++) {
				switch (ast.getChild(i).getType()) {
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND: {
					Long id = Long.decode(ast.getChild(i).getChild(0).getChild(0).getText());
					r.addSingles(new ExpressionId(id));
					break;
				}
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC: {
					String sid = ast.getChild(i).getChild(0).getChild(0).getChild(0).getText();
					Long id = Long.decode(sid);
					r.addDescendants(new ExpressionId(id));
					break;
				}
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC_SELF: {
					String sid = ast.getChild(i).getChild(0).getChild(0).getChild(0).getText();
					Long id = Long.decode(sid);
					r.addDescendantsAndSelf(new ExpressionId(id));
					break;
				}
				}
			}
			return r;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC_SELF: {
			MRCMRestriction r = new MRCMRestriction();
			String sid = ast.getChild(0).getChild(0).getChild(0).getText();
			Long id = Long.decode(sid);
			r.addDescendantsAndSelf(new ExpressionId(id));
			return r;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.ALL: {
			MRCMRestriction r = new MRCMRestriction();
			r.addDescendantsAndSelf(new ExpressionId((long) 138875005));
		}
		default: {
			return null;
		}

		}

	}

	@Override
	public boolean validate(ExpressionId subject, ExpressionId relationship,
			ExpressionId object) {
		OWLReasoner reasoner = repo.getReasoner();
		
		for(int i = 0; i < mrcmList.size(); i++) {
			
		}
		
		return false;
	}
}
