package com.practice.kafka.producer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

class FileProducerTest {

    @Test
    void joiningTest() {

        String line = "P001,ord0, P001, Cheese Pizza, Erick Koelpin, (235) 592-3785 x9190, 6373 Gulgowski Path, 2022-07-14 12:09:33";
        String[] tokens = line.split(",");

        String key = tokens[0];
        String collect = Arrays.stream(tokens).skip(1)
                .collect(Collectors.joining(","));

        Assertions.assertEquals("ord0, P001, Cheese Pizza, Erick Koelpin, (235) 592-3785 x9190, 6373 Gulgowski Path, 2022-07-14 12:09:33", collect);

    }
}