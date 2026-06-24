package dev.ayan;

/**
 * Shared wire protocol constants used by MiniKafka clients and broker.
 *
 * All requests start with a 1-byte command.
 * All responses start with a 1-byte response code.
 */
public interface Protocol {

    // Client commands
    byte CMD_SEND = 1;
    byte CMD_FETCH = 2;

    // Broker response codes
    byte RES_OK = 0;
    byte RES_ERROR = 1;

    // Safety limits for request validation
    int MAX_TOPIC_NAME_BYTES = 255;
    int MAX_PAYLOAD_BYTES = 1024 * 1024; // 1 MiB
    int MAX_FETCH_ENTRIES = 10_000;
}