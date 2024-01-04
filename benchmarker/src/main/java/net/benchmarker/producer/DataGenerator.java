package net.benchmarker.producer;

import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;

import java.util.Arrays;

public class DataGenerator {
    static final Xoroshiro128PlusRandom keyGeneratorRandom = new Xoroshiro128PlusRandom(21);

    static final char[] alphabet = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    static String generate(final int size) {
        final char[] chars = new char[size];
        Arrays.fill(chars, alphabet[keyGeneratorRandom.nextInt(alphabet.length)]);
        return new String(chars);
    }
}
