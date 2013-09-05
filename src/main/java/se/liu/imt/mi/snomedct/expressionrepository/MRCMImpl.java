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
import se.liu.imt.mi.snomedct.expression.tools.SnomedCTParser;
import se.liu.imt.mi.snomedct.expressionrepository.api.ConceptModelException;
import se.liu.imt.mi.snomedct.expressionrepository.api.MRCM;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;

/**
 * An implementation of the MRCM as it exists at the time being (Aug 2013). This
 * implementation assumes that domains and ranges can only be expressed as a
 * union of either single concepts, descendants of a single concepts, or
 * descendants of a single concept including itself. No other expressivity is
 * allowd.
 * 
 * @author daniel
 * 
 */
public class MRCMImpl implements MRCM {

	static int ACTIVE = 2;

	static int DOMAIN = 6;

	private static final Logger log = Logger.getLogger(MRCMImpl.class);
	static int RANGE = 7;
	static int RELATIONSHIP = 5;
	private List<MRCMEntry> mrcmList = null;

	private ExpressionRepositoryImpl repo = null;

	public MRCMImpl(ExpressionRepositoryImpl repo) {
		super();
		this.repo = repo;
		mrcmList = new ArrayList<MRCMEntry>();
	}

	private class MRCMEntry {
		List<MRCMRestriction> domain;
		List<MRCMRestriction> range;
		ExpressionId relationship;

		public MRCMEntry(ExpressionId relationship) {
			super();
			this.relationship = relationship;
			this.domain = new ArrayList<MRCMRestriction>();
			this.range = new ArrayList<MRCMRestriction>();
		}

		public List<MRCMRestriction> getDomain() {
			return domain;
		}

		public List<MRCMRestriction> getRange() {
			return range;
		}

		public ExpressionId getRelationship() {
			return relationship;
		}
	}

	private class MRCMRestriction {
		/**
		 * @param type
		 * @param id
		 */
		public MRCMRestriction(MRCMRestrictionType type, ExpressionId id) {
			super();
			this.type = type;
			this.id = id;
		}

		MRCMRestrictionType type;
		ExpressionId id;

		/**
		 * @return the type
		 */
		public MRCMRestrictionType getType() {
			return type;
		}

		/**
		 * @param type
		 *            the type to set
		 */
		public void setType(MRCMRestrictionType type) {
			this.type = type;
		}

		/**
		 * @return the id
		 */
		public ExpressionId getId() {
			return id;
		}

		/**
		 * @param id
		 *            the id to set
		 */
		public void setId(ExpressionId id) {
			this.id = id;
		}
	}

	private enum MRCMRestrictionType {
		DescendantsAndSelf, Descendants, Single
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
						MRCMEntry entry = new MRCMEntry(relationship);

						parseMRCMEntry(entry.getDomain(), items[DOMAIN]);
						parseMRCMEntry(entry.getRange(), items[RANGE]);

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

	/**
	 * Parses a SNOMED CT Query Specification string as it appears in the MRCM representation 
	 * 
	 * @param list list of restrictions, the list is passed by reference
	 * @param s <code>String</code> to be parsed
	 * @throws Exception pass on of exceptions from parsing 
	 */
	private void parseMRCMEntry(List<MRCMRestriction> list, String s)
			throws Exception {

		Tree ast = SnomedCTParser.parseQuery(s);

		// CharStream input = new ANTLRStringStream(s);
		// SCTExpressionLexer lexer = new SCTExpressionLexer(input);
		// CommonTokenStream tokens = new CommonTokenStream(lexer);
		// SCTExpressionParser parser = new SCTExpressionParser(tokens);
		// SCTExpressionParser.query_return result = null;
		// try {
		// result = parser.query();
		// } catch (RecognitionException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }
		//
		// Tree ast = (Tree) result.getTree();

		log.debug(SCTExpressionParser.tokenNames[ast.getType()]);

		// TestSCTExpressionParser.printTree(ast, 0);

		switch (ast.getType()) {
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND: {
			// a single expression (id)
			// TODO: what if a post-coordination expression is here, probably not allowed
			Long id = Long.decode(ast.getChild(0).getChild(0).getText());
			list.add(new MRCMRestriction(MRCMRestrictionType.Single,
					new ExpressionId(id)));
			return;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.UNION: {
			for (int i = 0; i < ast.getChildCount(); i++) {
				switch (ast.getChild(i).getType()) {
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND: {
					Long id = Long.decode(ast.getChild(i).getChild(0)
							.getChild(0).getText());
					list.add(new MRCMRestriction(MRCMRestrictionType.Single, new ExpressionId(id)));
					break;
				}
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC: {
					String sid = ast.getChild(i).getChild(0).getChild(0)
							.getChild(0).getText();
					Long id = Long.decode(sid);
					list.add(new MRCMRestriction(MRCMRestrictionType.Descendants, new ExpressionId(id)));
					break;
				}
				case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC_SELF: {
					String sid = ast.getChild(i).getChild(0).getChild(0)
							.getChild(0).getText();
					Long id = Long.decode(sid);
					list.add(new MRCMRestriction(MRCMRestrictionType.DescendantsAndSelf, new ExpressionId(id)));
					break;
				}
				}
			}
			return;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC_SELF: {
			String sid = ast.getChild(0).getChild(0).getChild(0).getText();
			Long id = Long.decode(sid);
			list.add(new MRCMRestriction(MRCMRestrictionType.DescendantsAndSelf, new ExpressionId(id)));
			return;
		}
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC: {
			String sid = ast.getChild(0).getChild(0).getChild(0).getText();
			Long id = Long.decode(sid);
			list.add(new MRCMRestriction(MRCMRestrictionType.Descendants, new ExpressionId(id)));
			return;
		}
		
		case se.liu.imt.mi.snomedct.expression.SCTExpressionParser.ALL: {
			list.add(new MRCMRestriction(
					MRCMRestrictionType.DescendantsAndSelf, new ExpressionId(
							(long) 138875005)));
			return;
		}
		default: {
			return;
		}

		}

	}

	/* (non-Javadoc)
	 * @see se.liu.imt.mi.snomedct.expressionrepository.api.MRCM#validate(se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId, se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public boolean validate(ExpressionId subject, ExpressionId relationship,
			ExpressionId object) throws ConceptModelException {

		for (MRCMEntry e : mrcmList) {
			ExpressionId entryRelationship = e.getRelationship();
			if(relationship.equals(entryRelationship) || isSubsumedBy(relationship, entryRelationship))
				continue;
			
			if(!matchRestrictions(subject, e.getDomain()))
				throw new ConceptModelException("Domain does not match, id " + subject.toString());
			
			if(!matchRestrictions(object, e.getRange()))
				throw new ConceptModelException("Range does not match, id " + object.toString());
			

		}

		return true;
	}
	
	/**
	 * Match, depending of type of restriction, an <code>ExpressionId</code> against a list of restrictions
	 * 
	 * @param id The expression ID to be matched
	 * @param list The list of restrictions
	 * @return true if there is a match between expression ID and any of the restrictions on the list
	 */
	private boolean matchRestrictions(ExpressionId id, List<MRCMRestriction> list) {
		
		boolean match = false;
		for(MRCMRestriction r : list) {
			switch (r.getType()) {
			case Single:
				if(id.equals(r.getId()))
					match = true;
				break;
			case Descendants:
				if(isSubsumedBy(id, r.getId()))
					match = true;
				break;
			case DescendantsAndSelf:
				if(id.equals(r.getId()) || isSubsumedBy(id, r.getId()))
					match = true;
				break;
			default:
				break;
			}
		}
		
		return match;
	}

	private boolean isSubsumedBy(ExpressionId e1, ExpressionId e2) {
		// TODO: replace with functioning method
		return true;
	}
}
