/**
 * 
 */
package com.sa.storm.exception;

/**
 * @author lewis
 * 
 */
public class InvalidInputException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4033557410880830537L;

	/**
	 * 
	 */
	public InvalidInputException() {
	}

	/**
	 * @param message
	 */
	public InvalidInputException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public InvalidInputException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InvalidInputException(String message, Throwable cause) {
		super(message, cause);
	}

}
