/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.api;

/**
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class NonExistingIdException extends Exception {

	/**
	 * 
	 */
	public NonExistingIdException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public NonExistingIdException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public NonExistingIdException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NonExistingIdException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public NonExistingIdException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
