/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import se.liu.imt.mi.snomedct.expressionrepository.ExpressionRepositoryImpl;
import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;

/**
 * An implementation of the <code>DataStore</code> interface for the PostgreSQL
 * database management system including service methods.
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class DataStoreService extends DataStore {

	/**
	 * Logger
	 */
	private static final Logger log = Logger
			.getLogger(ExpressionRepositoryImpl.class);

	/**
	 * Utility to restore expression repository database back to a certain date
	 * 
	 * @param args
	 *            The arguments are never used.
	 * @throws Exception
	 *             If something goes wrong.
	 */
	public static void main(String[] args) throws Exception {
		// initialize configuration
		Configuration config = null;
		config = new XMLConfiguration("config.xml");

		String url = config.getString("database.url");
		String username = config.getString("database.username");
		String password = config.getString("database.password");

		String date = "2012-08-01";

		DataStoreService dss = new DataStoreService(url, username, password);
		DateFormat formatter = new SimpleDateFormat("YY-MM-DD");
		log.debug("Connected to database server");
		dss.restoreDataStore(formatter.parse(date));
		log.debug("Restored to " + date);
	}

	/**
	 * A <code>PreparedStatement</code> restore the dbms to a previous state by
	 * removing rows in the expressions table.
	 */
	private final PreparedStatement restoreDataStoreExpressionsDelete;

	/**
	 * A <code>PreparedStatement</code> restore the dbms to a previous state by
	 * updating rows in the expressions table.
	 */
	private final PreparedStatement restoreDataStoreExpressionsUpdate;

	/**
	 * A <code>PreparedStatement</code> restore the dbms to a previous state by
	 * removing rows in the transitiveclosure table.
	 */
	private final PreparedStatement restoreDataStoreTransitiveclosureDelete;

	/**
	 * A <code>PreparedStatement</code> restore the dbms to a previous state by
	 * updating rows in the transitiveclosure table.
	 */
	private final PreparedStatement restoreDataStoreTransitiveclosureUpdate;

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
	public DataStoreService(String url, String userName, String password)
			throws DataStoreException {
		super(url, userName, password);
		try {
			restoreDataStoreExpressionsDelete = super.con
					.prepareStatement("DELETE FROM expressions WHERE starttime > ?;");
			restoreDataStoreExpressionsUpdate = super.con
					.prepareStatement("UPDATE expressions SET endtime = NULL WHERE endtime > ?;");
			restoreDataStoreTransitiveclosureDelete = super.con
					.prepareStatement("DELETE FROM transitiveclosure WHERE starttime > ?;");
			restoreDataStoreTransitiveclosureUpdate = super.con
					.prepareStatement("UPDATE transitiveclosure SET endtime = NULL WHERE endtime > ?;");
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/**
	 * Restore the data store to the state at a specific time.
	 * 
	 * @param time
	 *            The time to restore the data store to.
	 * @throws DataStoreException
	 *             Thrown if there are any problem with the data store.
	 * @throws NullPointerException
	 *             Thrown if no time to restore t is given.
	 */
	public void restoreDataStore(final Date time) throws DataStoreException,
			NullPointerException {
		if (time == null) {
			throw new NullPointerException(
					"The time to restore the data store to must be given.");
		}
		final Timestamp sqlTimestamp = new Timestamp(time.getTime());

		try {
			super.con.setAutoCommit(false);
			restoreDataStoreExpressionsDelete.setTimestamp(1, sqlTimestamp);
			restoreDataStoreExpressionsDelete.executeUpdate();
			restoreDataStoreExpressionsUpdate.setTimestamp(1, sqlTimestamp);
			restoreDataStoreExpressionsUpdate.executeUpdate();
			restoreDataStoreTransitiveclosureDelete.setTimestamp(1,
					sqlTimestamp);
			restoreDataStoreTransitiveclosureDelete.executeUpdate();
			restoreDataStoreTransitiveclosureUpdate.setTimestamp(1,
					sqlTimestamp);
			restoreDataStoreTransitiveclosureUpdate.executeUpdate();
			super.con.commit();
			super.con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql.DataStore
	 * #finalize()
	 */
	@Override
	public void finalize() throws Throwable {
		super.finalize();
	}

}
