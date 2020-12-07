package com.codecubic.exception;

public class NotImplemtException extends RuntimeException {

    public NotImplemtException(RuntimeException e) {
        super(e);
    }

    public NotImplemtException() {

    }
}
