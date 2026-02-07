package com.davidvlijmincx.lio.api;

import java.nio.file.OpenOption;

public enum LinuxOpenOptions implements OpenOption {
    READ((byte) 0),
    READ_DIRECT((byte) 16384),
    WRITE((byte) 1),
    WRITE_DIRECT((byte) (1 | 16384)),
    CREATE((byte) 64),
    READ_WRITE((byte) 2),
    READ_WRITE_DIRECT((byte) (2 | 16384));

    private byte flag;

    LinuxOpenOptions(byte value) {
        flag = value;
    }

    byte getValue() {
        return flag;
    }
}
