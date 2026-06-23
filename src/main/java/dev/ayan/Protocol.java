package dev.ayan;

public interface Protocol {
    byte CMD_SEND  = 1;
    byte CMD_FETCH = 2;

    // Response codes
    byte RES_OK    = 0;
    byte RES_ERROR = 1;
}