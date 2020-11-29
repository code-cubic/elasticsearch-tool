package com.codecubic.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@ToString
@NoArgsConstructor
@Getter
@Setter
public class Tuple<K, V> implements Serializable {
    private K v1;
    private V v2;

    public Tuple(K first, V second) {
        this.v1 = first;
        this.v2 = second;
    }
}
