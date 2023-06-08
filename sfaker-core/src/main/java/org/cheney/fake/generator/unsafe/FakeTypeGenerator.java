package org.cheney.fake.generator.unsafe;

import javax.activation.UnsupportedDataTypeException;

public interface FakeTypeGenerator {

    public void init() throws UnsupportedDataTypeException;

    public void genIntoWriter(int ordinal) throws UnsupportedDataTypeException;
}
