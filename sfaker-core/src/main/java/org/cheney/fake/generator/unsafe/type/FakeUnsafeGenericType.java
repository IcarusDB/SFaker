package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import javax.activation.UnsupportedDataTypeException;

public class FakeUnsafeGenericType extends FakeUnsafeType {

    private FakeUnsafeType type;

    public FakeUnsafeGenericType(UnsafeWriter writer, DataType type) {
        super(writer, type);
    }

    public static boolean isPrimitive(DataType type) {
        if (type == DataTypes.ByteType) {
            return true;
        }

        if (type == DataTypes.BooleanType) {
            return true;
        }

        if (type == DataTypes.IntegerType) {
            return true;
        }

        if (type == DataTypes.LongType) {
            return true;
        }

        if (type == DataTypes.FloatType) {
            return true;
        }

        if (type == DataTypes.DoubleType) {
            return true;
        }

        if (type == DataTypes.StringType) {
            return false;
        }

        if (type instanceof ArrayType) {
            return false;
        }

        if (type instanceof StructType) {
            return false;
        }

        if (type instanceof MapType) {
            return false;
        }

        return false;
    }

    @Override
    public void init() throws UnsupportedDataTypeException {
        if (type() == DataTypes.ByteType) {
            this.type = new FakeUnsafeByteType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.BooleanType) {
            this.type = new FakeUnsafeBooleanType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.IntegerType) {
            this.type = new FakeUnsafeIntType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.LongType) {
            this.type = new FakeUnsafeLongType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.FloatType) {
            this.type = new FakeUnsafeFloatType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.DoubleType) {
            this.type = new FakeUnsafeDoubleType(writer(), type());
            this.type.init();
            return;
        }

        if (type() == DataTypes.StringType) {
            this.type = new FakeUnsafeStringType(writer(), type());
            this.type.init();
            return;
        }

        if (type() instanceof ArrayType) {
            this.type = new FakeUnsafeArrayType(writer(), type());
            this.type.init();
            return;
        }

        if (type() instanceof StructType) {
            this.type = new FakeUnsafeRowType(writer(), type());
            this.type.init();
            return;
        }

        if (type() instanceof MapType) {
            this.type = new FakeUnsafeMapType(writer(), type());
            this.type.init();
            return;
        }

        throw new UnsupportedDataTypeException(String.format("%s is unsupported.", type()));
    }

    @Override
    public void genIntoWriter(int ordinal) throws UnsupportedDataTypeException {
        if (this.type != null) {
            this.type.genIntoWriter(ordinal);
        }
    }

    @Override
    public void constructCode(Context ctx) {
        if (this.type != null) {
            this.type.constructCode(ctx);
        }
    }

    @Override
    public void initCode(Context ctx) {
        if (this.type != null) {
            this.type.initCode(ctx);
        }
    }

    @Override
    public void genCode(Context ctx, String ordinalName) {
        if (this.type != null) {
            this.type.genCode(ctx, ordinalName);
        }
    }
}
