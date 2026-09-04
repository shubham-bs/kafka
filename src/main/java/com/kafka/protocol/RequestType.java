package com.kafka.protocol;

public enum RequestType {

    PING(1),
    PRODUCE(2),
    FETCH(3);

    private final int code;

    RequestType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RequestType fromCode(int code) {
        for (RequestType type : values()) {
            if (type.code == code) return type;
        }

        throw new IllegalArgumentException("Unknown request type: " + code);
    }
}