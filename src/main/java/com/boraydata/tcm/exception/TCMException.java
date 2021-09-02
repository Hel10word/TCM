package com.boraydata.tcm.exception;

/**
 * @author bufan
 * @data 2021/8/25
 */
public class TCMException extends RuntimeException {

    private static final long serialVersionUID = 8480450962311247736L;

    public TCMException(String message) {
        super(message);
    }

    public TCMException(String message, Throwable cause) {
        super(message, cause);
    }
}
