package com.boraydata.cdc.tcm.exception;

/**
 * @author bufan
 * @date 2021/8/25
 */
public class TCMException extends RuntimeException {

    private static final long serialVersionUID = 8480450962311247736L;

    public TCMException(String message) {
        super(message);
    }

    public TCMException(String message, Throwable cause) {
        super(message+"   Exception:"+cause.getMessage(), cause);
    }
}
