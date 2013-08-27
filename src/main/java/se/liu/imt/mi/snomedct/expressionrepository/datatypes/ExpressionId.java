/**
 * 
 */
package se.liu.imt.mi.snomedct.expressionrepository.datatypes;

/**
 * @author Daniel Karlsson, daniel.karlsson@liu.se
 * @author Mikael Nyström, mikael.nystrom@liu.se
 * 
 */

public class ExpressionId extends Object {

	/**
	 * @param id
	 */
	public ExpressionId(Long id) {
		super();
		this.internalExpressionId = id;
	}

	/**
	 * 
	 */
	private Long internalExpressionId = null;

	public Long getId() {
		return internalExpressionId;
	}

	/**
	 * @return
	 */
	public boolean isPreCoordinated() {
		return internalExpressionId > 0;
	}

	/**
	 * @return
	 */
	public boolean isPostCoordinated() {
		return internalExpressionId <= 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((internalExpressionId == null) ? 0 : internalExpressionId
						.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExpressionId other = (ExpressionId) obj;
		if (internalExpressionId == null) {
			if (other.internalExpressionId != null)
				return false;
		} else if (!internalExpressionId.equals(other.internalExpressionId))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return internalExpressionId.toString();
	}

}
