/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.semanticweb.elk.owlapi.ElkReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.DLSyntaxDocumentFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDocumentFormat;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.Node;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;

//import au.csiro.snorocket.owlapi.SnorocketReasonerFactory;

//import au.csiro.snorocket.core.Snorocket;
//import au.csiro.snorocket.owlapi3.SnorocketReasoner;



import se.liu.imt.mi.snomedct.expression.tools.SNOMEDCTParserUtil;
import se.liu.imt.mi.snomedct.expression.tools.ExpressionSyntaxError;
import se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository;
import se.liu.imt.mi.snomedct.expressionrepository.api.NonExistingIdException;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStore;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.Expression;
import se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId;
import se.liu.imt.mi.snomedct.parser.SortedExpressionVisitor;
import org.semanticweb.owlapi.manchestersyntax.renderer.ManchesterOWLSyntaxObjectRenderer;
import org.semanticweb.owlapi.manchestersyntax.renderer.ManchesterOWLSyntaxPrefixNameShortFormProvider;

/**
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 * @author Mikael Nyström, mikael.nystrom@liu.se
 */
public class ExpressionRepositoryImpl extends Object implements
		ExpressionRepository {

	private OWLOntology ontology;
	private OWLDataFactory dataFactory;
	private OWLOntologyManager manager;
	private OWLReasoner reasoner;
	private DataStore dataStore;

	private static final Logger log = Logger
			.getLogger(ExpressionRepositoryImpl.class);
	Configuration config = null;

	/**
	 * Constructor for ExpressionRepository implementation. Initializes
	 * configuration management, logging, database and OWLAPI related
	 * components.
	 * 
	 * @throws Exception
	 *             Forwards any exception thrown during initialization
	 */
	public ExpressionRepositoryImpl() throws Exception {
		super();

		// initialize configuration
		try {
			config = new XMLConfiguration("config.xml");
			log.debug("Configuration in 'config.xml' loaded");
		} catch (Exception e) {
			log.debug("Exception", e);
			throw e;
		}

		// initialize data store
		try {
			String url = config.getString("database.url");
			String username = config.getString("database.username");
			String password = config.getString("database.password");
			dataStore = new se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore(
					url, username, password);
			log.debug("DataStore initialized");
		} catch (Exception e) {
			log.debug("Exception", e);
			throw e;
		}

		// initialize OWL API
		manager = OWLManager.createOWLOntologyManager();
		dataFactory = manager.getOWLDataFactory();

		// initialize ontology
		try {
			String url = config.getString("owlapi.url");
			log.debug("Starting to load ontology into memory");
			ontology = manager
					.loadOntologyFromOntologyDocument(IRI.create(url));
			log.debug("Finished loading ontology into memory");

			log.debug("Creating reasoner");
			OWLReasonerFactory reasonerFactory = new ElkReasonerFactory();
			reasoner = reasonerFactory.createReasoner(ontology);

		} catch (OWLOntologyCreationException e) {
			log.debug("Exception", e);
			throw e;
		}

		// add all existing expressions from expression table to ontology
		try {
			log.debug("Adding existing expressions from data store to ontology");
			Collection<Expression> expressions = dataStore
					.getAllExpressions(null);
			for (Expression ex : expressions) {
				SNOMEDCTParserUtil.parseExpressionToOWLAxiom(
						ex.getExpression(), ontology);
				// addExpressionToOntology(result, ex.getExpressionId());
			}
		} catch (Exception e) {
			log.debug("Exception", e);
			// throw e;
		}

		// classify ontology
		log.debug("Starting classification of ontology");
		reasoner.precomputeInferences();//org.semanticweb.owlapi.reasoner.InferenceType.CLASS_HIERARCHY);
		log.debug("Finished classifying ontology");
		
		log.debug("No. of axioms = " + ontology.getAxiomCount());

	}

	private ExpressionId getExpressionID(ParseTree tree) {
		// generate sorted expression string
		SortedExpressionVisitor sortVisitor = new SortedExpressionVisitor();
		String sortedExpression = sortVisitor.visit(tree);

		log.debug("sorted expression = " + sortedExpression);

		// use try block to implement transaction??
		try {
			// id of the current expression, either an existing or a new one
			ExpressionId expid = null;

			// check if the expression is a single code
			try {
				expid = new ExpressionId(new Long(sortedExpression));
			} catch (NumberFormatException nfe) {
				;
			} catch (Exception e) {
				// Should never happen!
				throw new ExpressionSyntaxError(e);
			}
			if (expid == null)
				// if the expression is not a plain number, check if the
				// expression exists in the repository
				expid = dataStore.getExpressionId(sortedExpression, null);

			// TODO: If the expression is a single number but not an expression
			// ID or SCTID, then an error should be thrown...

			if (expid != null) {// the expression is currently in the
								// expression repository
				log.debug("existing expression id = " + expid.toString());
				return expid;
			} else
				// add expression to expression table
				expid = dataStore.storeExpression(sortedExpression, null);

			log.debug("new expression id = " + expid.toString());

			// generate OWL expression and create new class for the expression
			// add axiom to ontology and classify
			OWLClass new_pc_concept = manager.getOWLDataFactory().getOWLClass(
					IRI.create(SNOMEDCTParserUtil.PC_IRI + expid.toString()));

			SNOMEDCTParserUtil.parseExpressionToOWLAxiom(tree, ontology,
					new_pc_concept, false);
			
			reasoner.flush();
			reasoner.precomputeInferences();

			// check for equivalent classes
			Node<OWLClass> equivalentClasses = reasoner
					.getEquivalentClasses(new_pc_concept);
			ExpressionId eqExpid = null;
			if (equivalentClasses.getSize() != 0) {
				for (OWLClass cl : equivalentClasses.getEntities()) {
					ExpressionId eqExpidTemp = new ExpressionId(
							getIDFromOWLElement(cl));
					log.debug("equivalent expression id = "
							+ eqExpidTemp.toString());
					if (!expid.equals(eqExpidTemp)) {
						eqExpid = eqExpidTemp;
						if (eqExpid.isPreCoordinated())
							break;
					}
				}
			}
			if (eqExpid != null)
				// if there is at least one equivalent expression or
				// pre-coordinated
				// concept then store the equivalence in the repository
				dataStore.storeExpressionEquivalence(expid, eqExpid, null);
			else {
				// if there are no equivalent expressions or pre-cordinated
				// concepts, add direct super- and sub classes to the data store
				NodeSet<OWLClass> superClasses = reasoner.getSuperClasses(
						new_pc_concept, true);
				HashSet<ExpressionId> parents = new HashSet<ExpressionId>();
				if (!superClasses.isEmpty()) {
					for (Node<OWLClass> c : superClasses) {
						for (OWLClass cl : c.getEntities()) {
							log.debug("parent id = " + cl.toString());
						}
						ExpressionId id = new ExpressionId(
								getIDFromOWLElement(c
										.getRepresentativeElement()));
						log.debug("parent id = " + id.toString());
						if (id.getId() != 0)
							parents.add(id);
					}
				}
				NodeSet<OWLClass> subClasses = reasoner.getSubClasses(
						new_pc_concept, true);
				HashSet<ExpressionId> children = new HashSet<ExpressionId>();
				if (!subClasses.isEmpty()) {
					for (Node<OWLClass> c : subClasses) {
						ExpressionId id = new ExpressionId(
								getIDFromOWLElement(c
										.getRepresentativeElement()));
						for (OWLClass cl : c.getEntities()) {
							log.debug("child id = " + cl.toString());
						}
						log.debug("child id = " + id.toString());
						if (id.getId() != 0)
							children.add(id);
					}
				}
				dataStore.storeExpressionParentsAndChildren(expid, parents,
						children, null);
			}
			// return newly generated ID
			return expid;

		} catch (Exception e) {
			log.debug("Exception caught: " + e.getMessage());
			e.printStackTrace();
			// perform rollback;
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getExpressionID(java.lang.String)
	 */
	@Override
	public ExpressionId getExpressionID(String expression)
			throws ExpressionSyntaxError, NonExistingIdException {

		log.debug("expression = " + expression);

		ParseTree result = SNOMEDCTParserUtil.parseExpression(expression);

		return getExpressionID(result);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getExpression
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public String getExpression(ExpressionId id) throws NonExistingIdException {
		throw new NotImplementedException(
				"String getExpression(ExpressionId) not implemented in data store");
		// return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getDecendants
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public Collection<ExpressionId> getDecendants(ExpressionId id)
			throws DataStoreException, NonExistingIdException {
		return dataStore.getDescendants(id, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getChildren
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public Collection<ExpressionId> getChildren(ExpressionId id)
			throws DataStoreException, NonExistingIdException {
		return dataStore.getChildren(id, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getAncestors
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public Collection<ExpressionId> getAncestors(ExpressionId id)
			throws DataStoreException, NonExistingIdException {
		return dataStore.getAncestors(id, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.api.ExpressionRepository#
	 * getParents
	 * (se.liu.imt.mi.snomedct.expressionrepository.datatypes.ExpressionId)
	 */
	@Override
	public Collection<ExpressionId> getParents(ExpressionId id)
			throws DataStoreException, NonExistingIdException {
		return dataStore.getParents(id, null);
	}

	private Long getIDFromOWLElement(OWLClass c) {
		String name = c.toStringID();
		log.debug("OWLClass = " + name);
		Long id = (long) 0;
		try {
			id = new Long(name.substring(name.lastIndexOf('/') + 1));
		} catch (Exception e) {
			;
		}
		return id;
	}

	// probably not needed anymore...
	// private OWLClass addExpressionToOntology(Tree parseTree, ExpressionId
	// expid)
	// throws Exception {
	//
	// log.debug("expression id = " + expid.toString());
	//
	// // create OWL expression from parse tree
	// OWLClassExpression owlExpression = owlExpressionBuilder
	// .translateToOWL(parseTree);
	//
	// // create new class for the expression
	// OWLClass new_pc_concept = dataFactory
	// .getOWLClass(IRI
	// .create(se.liu.imt.mi.snomedct.expression.tools.SCTOWLExpressionBuilder.PC_IRI
	// + expid.toString()));
	//
	// // add equivalence axom to ontology
	// manager.addAxiom(ontology, dataFactory.getOWLEquivalentClassesAxiom(
	// new_pc_concept, owlExpression));
	//
	// // classify ontology
	// reasoner.flush();
	// reasoner.precomputeInferences();
	//
	// return new_pc_concept;
	// }

	private String printOWLExpression(OWLClassExpression e) {
		StringWriter sw = new StringWriter();
		OWLDocumentFormat of = new DLSyntaxDocumentFormat();
		ManchesterOWLSyntaxPrefixNameShortFormProvider ssfp = new ManchesterOWLSyntaxPrefixNameShortFormProvider(
				of); // new SimpleShortFormProvider();
		ManchesterOWLSyntaxObjectRenderer renderer = new ManchesterOWLSyntaxObjectRenderer(
				sw, ssfp);
		renderer.visit((OWLObjectIntersectionOf) e);

		return sw.toString();
	}

	@Override
	public Collection<ExpressionId> getSCTQueryResult(String queryExpression)
			throws Exception {

		/*
		 * log.debug("Received query: " + queryExpression);
		 * 
		 * SCTExpressionParser.query_return parseResult = null;
		 * 
		 * // parse string and throw ExpressionSyntaxError if unparsable
		 * CharStream input = new ANTLRStringStream(queryExpression);
		 * SCTExpressionLexer lexer = new SCTExpressionLexer(input);
		 * CommonTokenStream tokens = new CommonTokenStream(lexer);
		 * SCTExpressionParser parser = new SCTExpressionParser(tokens); try {
		 * parseResult = parser.query(); // Note difference in starting point //
		 * for parser as compared to parsing // for SCT expressions } catch
		 * (Exception e) { throw new ExpressionSyntaxError(e); } if (parseResult
		 * == null) throw new ExpressionSyntaxError(
		 * "Parse result is null. Should not happen ever!");
		 * 
		 * return processQuery((Tree) parseResult.getTree());
		 */

		return null;
	}

	/*
	 * private Collection<ExpressionId> processQuery(Tree ast) throws Exception
	 * { if (ast != null) { switch (ast.getType()) { case
	 * se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC_SELF: case
	 * se.liu.imt.mi.snomedct.expression.SCTExpressionParser.DESC: { if
	 * (ast.getChildCount() != 1 || ast.getChild(0).getType() !=
	 * se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND) throw new
	 * ExpressionSyntaxError(
	 * "Descendant may only take an SCT expression as argument, not a query expression"
	 * ); ExpressionId expid = getExpressionID(ast.getChild(0)); return
	 * getDecendants(expid); } case
	 * se.liu.imt.mi.snomedct.expression.SCTExpressionParser.UNION: {
	 * Collection<ExpressionId> set = new HashSet<ExpressionId>(); for (int i =
	 * 0; i < ast.getChildCount(); i++) {
	 * set.addAll(processQuery(ast.getChild(i))); } return set; } case
	 * se.liu.imt.mi.snomedct.expression.SCTExpressionParser.TOP_AND: {
	 * Collection<ExpressionId> c = new HashSet<ExpressionId>();
	 * c.add(getExpressionID(ast)); return c; } default: throw new
	 * Exception("Undetermined AST node type: " + ast.getType());
	 * 
	 * } } return null; }
	 */
	public OWLReasoner getReasoner() {
		return reasoner;
	}

	@Override
	public boolean isSubsumedNotEquivalent(ExpressionId id1, ExpressionId id2,
			Date time) throws DataStoreException {
		// TODO Auto-generated method stub
		return false;
	}
}
