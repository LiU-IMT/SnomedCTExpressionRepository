/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.api;

/**
 * 
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */
public class ExpressionAlreadyDefined extends Exception {

	/**
	 * 
	 */
	public ExpressionAlreadyDefined() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public ExpressionAlreadyDefined(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public ExpressionAlreadyDefined(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ExpressionAlreadyDefined(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ExpressionAlreadyDefined(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
