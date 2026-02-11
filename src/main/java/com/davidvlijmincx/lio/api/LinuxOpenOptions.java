package com.davidvlijmincx.lio.api;

import java.nio.file.OpenOption;

public enum LinuxOpenOptions implements OpenOption {
    READ(0),
    READ_DIRECT(0x4000),
    WRITE(1),
    WRITE_DIRECT(1 | 0x4000),
    CREATE(64),
    READ_WRITE(2),
    READ_WRITE_DIRECT(2 | 0x4000);

    private final int flag;

    LinuxOpenOptions(int value) {
        flag = value;
    }

    public int getValue() {
        return flag;
    }
}