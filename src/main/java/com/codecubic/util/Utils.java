package com.codecubic.util;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

@Slf4j
public class Utils {
    public static void close(Closeable obj) {
        if (obj == null)
            return;
        try {
            obj.close();
        } catch (Exception e) {
            log.error("", e);
        }

    }
}
