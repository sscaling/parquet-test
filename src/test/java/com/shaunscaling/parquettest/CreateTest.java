package com.shaunscaling.parquettest;

import com.google.protobuf.Descriptors;
import com.shaunscaling.protobuftest.protos.AddressBookProtos;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.proto.CustomProtoWriteSupport;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class CreateTest {


    @Test
    public void test() throws Exception {
        AddressBookProtos.AddressBook addressBook = AddressBookProtos.AddressBook.parseFrom(new FileInputStream("src/test/resources/test.pb"));
        System.out.println(addressBook.toString());
//
//        AddressBookProtos.AddressBook.Builder addressBuilder = addressBook.newBuilderForType();
//        FieldMaskUtil.merge(FieldMaskUtil.fromString("people"), addressBook, addressBuilder);
//        System.out.println(addressBuilder.toString());

        // Convert to parquet + Write Out
        CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

        // TODO: understand these parameters more (probably need to use existing values?)
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;

        Path outputPath = new Path("test.parquet");

        try (ProtoParquetWriter<AddressBookProtos.AddressBook> parquetWriter = new ProtoParquetWriter<>(outputPath,
                AddressBookProtos.AddressBook.class, compressionCodecName, blockSize, pageSize)) {


            parquetWriter.write(addressBook);
        }
    }

    @Test
    public void testReadParquet() throws IOException {
//        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path("test.parquet")).build();
//        GenericRecord nextRecord = reader.read();
//        System.out.println(nextRecord.toString());

        Path path = new Path("addressconvert.parquet");

        Configuration conf = new Configuration();

        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

            PageReadStore pages = null;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    System.out.println("Number of rows: " + rows);

                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int i = 0; i< rows; i++) {
                        final Group g = (Group) recordReader.read();
                        printGroup(g);

                        // TODO Compare to System.out.println(g);
                    }
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
    }

    private static void printGroup(Group g) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(fieldName + " " + g.getValueToString(field, index));
                } else {
                    System.out.println(fieldType);
                }
            }
        }
        System.out.println("");
    }


    @Test
    public void testFilter() throws IOException {

        AddressBookProtos.AddressBook addressBook = AddressBookProtos.AddressBook.parseFrom(new FileInputStream("src/test/resources/test.pb"));
        System.out.println(addressBook.toString());

        AddressBookProtos.PersonOrBuilder personBuilder = addressBook.getPeopleOrBuilder(0);
        AddressBookProtos.Person firstPerson = addressBook.getPeople(0);
//        Descriptors.FieldDescriptor id = firstPerson.getDescriptorForType().findFieldByName("id");
        Descriptors.FieldDescriptor id = addressBook.getDescriptorForType().findFieldByName("Person.id");
        System.out.println("id ? " + id);
        System.out.println(firstPerson.getField(id));

    }



    /**
     * TODO: try string
     * @throws Exception
     */
    @Test
    public void testConvertAddressBook() throws Exception {
        String filename = "addressconvert.parquet";
        Files.deleteIfExists(Paths.get(filename));
        Files.deleteIfExists(Paths.get(".addressconvert.parquet.crc"));

        AddressBookProtos.AddressBook addressBook = AddressBookProtos.AddressBook.parseFrom(new FileInputStream("src/test/resources/test.pb"));

        // define parquet schema (only
        GroupType personSchema = new GroupType(Type.Repetition.REPEATED, "people",
//                new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, "mapped_name"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "mapped_id")
        );

        MessageType rootSchema = new MessageType("protobuftest", personSchema);

        // need a builder to create an instance
        // key here is custom builder provides a custom WriteSupport impl
        Builder writerBuilder = new Builder(new Path(filename)).withType(rootSchema);

        // attempt to write out date to parquet file
        try (ParquetWriter<AddressBookProtos.AddressBook> writer = writerBuilder.withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            writer.write(addressBook);
            System.out.println("Data size ? " + writer.getDataSize());
        }

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filename)).build();
        GenericRecord nextRecord = reader.read();
        System.out.println(nextRecord.toString());

        System.out.println("done");
    }


    public static class Builder extends ParquetWriter.Builder<AddressBookProtos.AddressBook, Builder> {
        private MessageType type = null;
        private Map<String, String> extraMetaData = new HashMap<String, String>();

        private Builder(Path file) {
            super(file);
        }

        public Builder withType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<AddressBookProtos.AddressBook> getWriteSupport(Configuration conf) {
            return new CustomProtoWriteSupport(type);
//            return new ProtoWriteSupport<>(AddressBookProtos.AddressBook.class);
        }

    }
}
