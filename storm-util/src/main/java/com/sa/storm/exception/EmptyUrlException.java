/**
 * 
 */
package com.sa.storm.exception;

/**
 * Usage: Exception to be thrown when specified URL is null or empty.
 * 
 * @author lewis
 * 
 */
public class EmptyUrlException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7434810740021731048L;

	/**
	 * 
	 */
	public EmptyUrlException() {
	}

	/**
	 * @param message
	 */
	public EmptyUrlException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public EmptyUrlException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public EmptyUrlException(String message, Throwable cause) {
		super(message, cause);
	}

}
