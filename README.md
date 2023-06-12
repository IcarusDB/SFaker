# SFaker
<img src="docs/logo/sfaker_logo.svg" alt="SFaker logo" height="200px" align="center" />


![ApacheV2](https://img.shields.io/badge/license-Apache--2.0-blue)
[![Test with Maven](https://github.com/CheneyYin/SFaker/actions/workflows/maven_test.yml/badge.svg)](https://github.com/CheneyYin/SFaker/actions/workflows/maven_test.yml)
---
SFaker is one data generator. It implemented with Spark DataSourceV2. SFaker can generate rows according to specified schemas. 

## Features
| Feature                             |     |
|-------------------------------------|-----|
| Support Batch                       | ✅   |
| Support Stream                      | TBD |
| Support DataFrameReader API         | ✅   |
| Support Spark SQL Create Statement  | ✅   |
| Support Unsafe Row                  | ✅   |
| Support Codegen                     | ✅   |
| Support Limit Push Down             | ✅   |
| Support Columns Pruning             | ✅   |

## Types
Support spark sql types, more details about types click [here](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).

| Spark Type        |     |
|-------------------|-----|
| Byte              | ✅   |
| Short             | ✅   |
| Integer           | ✅   |
| Long              | ✅   |
| Float             | ✅   |
| Double            | ✅   |
| Decimal           | TBD |
| String            | ✅   |
| Varchar           | TBD |
| Char              | TBD |
| Binary            | TBD |
| Boolean           | ✅   |
| Date              | TBD |
| Timestamp         | TBD |
| TimestampNTZ      | TBD |
| YearMonthInterval | TBD |
| DayTimeInterval   | TBD |
| *Array*           | ✅   |
| *Map*             | ✅   |
| *Struct*          | ✅   |

## Config

| Conf                                          | Type     | Default | Description                                                                                                   |
|-----------------------------------------------|----------|---------|---------------------------------------------------------------------------------------------------------------|
| `spark.sql.fake.source.unsafe.row.enable`     | Boolean  | false   | If `true`, all row generated will been stored in `UnsafeRow`.                                                 |
| `spark.sql.fake.source.unsafe.codegen.enable` | Boolean  | false   | If `true`, the row-generated process, which produce rows according to schema, will been executed in JIT mode. |
| `spark.sql.fake.source.partitions`            | Integer  | 1       | Number of source partitions.                                                                                  |
| `spark.sql.fake.source.rowsTotalSize`         | Integer  | 8       | Number of rows generated according to schema.                                                                 |

## Use Cases
### DataFrameReader API
```scala
val schema = new StructType()
  .add("id", DataTypes.IntegerType)
  .add("sex", DataTypes.BooleanType)
  .add("roles", DataTypes.createArrayType(DataTypes.StringType));

val df = spark.read
  .format("FakeSource")
  .schema(schema)
  .option(FakeSourceProps.CONF_ROWS_TOTAL_SIZE, 100)
  .option(FakeSourceProps.CONF_PARTITIONS, 1)
  .option(FakeSourceProps.CONF_UNSAFE_ROW_ENABLE, true)
  .option(FakeSourceProps.CONF_UNSAFE_CODEGEN_ENABLE, true)
  .load();
```
### Spark SQL Create Statement
```scala
val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Case0")
      .config(
        "spark.sql.catalog.spark_catalog",
        classOf[FakeSourceCatalog].getName
      )
      .getOrCreate();
val df = spark.sql("""
          |create table fake (
          | id int,
          | sex boolean
          |)
          |using FakeSource
          |tblproperties (
          |spark.sql.fake.source.rowsTotalSize = 10000000,
          |spark.sql.fake.source.partitions = 1,
          |spark.sql.fake.source.unsafe.row.enable = true,
          |spark.sql.fake.source.unsafe.codegen.enable = true
          |)
          |""".stripMargin)
spark.sql("select id from fake limit 10").explain(true);
```

## License
[Apache 2.0 License.](LICENSE)
