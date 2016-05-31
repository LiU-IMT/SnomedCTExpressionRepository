/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import se.liu.imt.mi.snomedct.expressionrepository.datastore.DataStoreException;

/**
 * An implementation of the <code>DataStore</code> interface for the PostgreSQL database management system including
 * service methods.
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class DataStoreService extends DataStore {

	/**
	 * A <code>PreparedStatement</code> that restore the equivalents table in the dbms to a previous state by removing
	 * rows that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreEquivalentsDelete;
	/**
	 * A <code>PreparedStatement</code> that restore the expressions table in the dbms to a previous state by removing
	 * end times that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreEquivalentsEndTime;

	/**
	 * A <code>PreparedStatement</code> that restore the expressions table in the dbms to a previous state by removing
	 * rows that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreExpressionsDelete;
	/**
	 * A <code>PreparedStatement</code> that restore the equivalents table in the dbms to a previous state by removing
	 * end times that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreExpressionsEndTime;

	/**
	 * A <code>PreparedStatement</code> that restore the transitiveclosure table in the dbms to a previous state by
	 * removing rows that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreTransitiveclosureDelete;
	/**
	 * A <code>PreparedStatement</code> that restore the transitiveclosure table in the dbms to a previous state by
	 * removing end times that have been inserted after a specific timestamp.
	 */
	private final PreparedStatement restoreTransitiveclosureEndTime;

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
	public DataStoreService(String url, String userName, String password) throws DataStoreException {
		super(url, userName, password);
		try {
			restoreEquivalentsDelete = super.con.prepareStatement("DELETE FROM equivalents WHERE starttime > ?;");
			restoreEquivalentsEndTime = super.con.prepareStatement(
					"UPDATE equivalents SET endtime = 'infinity' WHERE ? < endtime AND endtime < 'infinity';");

			restoreExpressionsDelete = super.con.prepareStatement("DELETE FROM expressions WHERE starttime > ?;");
			restoreExpressionsEndTime = super.con.prepareStatement(
					"UPDATE expressions SET endtime = 'infinity' WHERE ? < endtime AND endtime < 'infinity';");

			restoreTransitiveclosureDelete = super.con
					.prepareStatement("DELETE FROM transitiveclosure WHERE starttime > ?;");
			restoreTransitiveclosureEndTime = super.con.prepareStatement(
					"UPDATE transitiveclosure SET endtime = 'infinity' WHERE ? < endtime AND endtime < 'infinity';");

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
	public void restoreDataStore(final Date time) throws DataStoreException, NullPointerException {
		if (time == null) {
			throw new NullPointerException("The time to restore the data store to must be given.");
		}
		final Timestamp sqlTimestamp = new Timestamp(time.getTime());

		try {
			super.con.setAutoCommit(false);
			restoreEquivalentsDelete.setTimestamp(1, sqlTimestamp);
			restoreEquivalentsDelete.executeUpdate();
			restoreEquivalentsEndTime.setTimestamp(1, sqlTimestamp);
			restoreEquivalentsEndTime.executeUpdate();
			restoreExpressionsDelete.setTimestamp(1, sqlTimestamp);
			restoreExpressionsDelete.executeUpdate();
			restoreExpressionsEndTime.setTimestamp(1, sqlTimestamp);
			restoreExpressionsEndTime.executeUpdate();
			restoreTransitiveclosureDelete.setTimestamp(1, sqlTimestamp);
			restoreTransitiveclosureDelete.executeUpdate();
			restoreTransitiveclosureEndTime.setTimestamp(1, sqlTimestamp);
			restoreTransitiveclosureEndTime.executeUpdate();
			super.con.commit();
			super.con.setAutoCommit(true);
		} catch (SQLException e) {
			throw new DataStoreException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.liu.imt.mi.snomedct.expressionrepository.datastore.postgresql. DataStore #finalize()
	 */
	@Override
	public void finalize() throws Throwable {
		super.finalize();
	}

}
