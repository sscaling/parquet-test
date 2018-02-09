package org.apache.parquet.proto;

import com.google.protobuf.*;
import com.shaunscaling.protobuftest.protos.AddressBookProtos;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CustomProtoWriteSupport extends WriteSupport<AddressBookProtos.AddressBook> {
//    private MessageWriter messageWriter;
    private MessageType rootSchema;
    private RecordConsumer recordConsumer;
    private CustomMessageWriter<AddressBookProtos.AddressBook> customWriter;

    public CustomProtoWriteSupport(MessageType rootSchema) {
        this.rootSchema = rootSchema;
    }


    @Override
    public String getName() {
        return "mappedprotobuf";
    }

    /**
     * Writes Protocol buffer to parquet file.
     * @param record instance of Message.Builder or Message.
     * */
    @Override
    public void write(AddressBookProtos.AddressBook record) {
        recordConsumer.startMessage();
        try {
//            messageWriter.writeTopLevelMessage(record);
            customWriter.writeTopLevelMessage(record);
        } catch (RuntimeException e) {
//            Message m = (record instanceof Message.Builder) ? ((Message.Builder) record).build() : (Message) record;
//            LOG.error("Cannot write message " + e.getMessage() + " : " + m);
            throw e;
        }
        recordConsumer.endMessage();
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }


    @Override
    public WriteSupport.WriteContext init(Configuration configuration) {
        Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(AddressBookProtos.AddressBook.class);
//        this.messageWriter = new MessageWriter(descriptor, rootSchema);

        this.customWriter = new CustomMessageWriter<>(rootSchema);

        Map<String, String> extraMetaData = new HashMap<String, String>();
        extraMetaData.put(ProtoReadSupport.PB_CLASS, AddressBookProtos.AddressBook.class.getName());
        extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(AddressBookProtos.AddressBook.class));
        return new WriteContext(rootSchema, extraMetaData);
    }

    class CustomFieldWriter {
        String fieldName;
        int index = -1;

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        /** sets index of field inside parquet message.*/
        void setIndex(int index) {
            this.index = index;
        }

        /** Used for writing repeated fields*/
        void writeRawValue(Object value) {

        }

        /** Used for writing nonrepeated (optional, required) fields*/
        void writeField(Object value) {
            recordConsumer.startField(fieldName, index);
            writeRawValue(value);
            recordConsumer.endField(fieldName, index);
        }
    }


    // FIXME: should be Flows concrete type, once I can test
    class CustomMessageWriter<T extends MessageOrBuilder> {

        private final MessageType schema;

        CustomMessageWriter(MessageType schema) {
            this.schema = schema;
        }

        public void writeTopLevelMessage(T value) {
            writeAllFields(value);
        }

        private void writeAllFields(T value) {
            System.out.println("--- write all fields ---");

//            List<Type> fields = schema.getFields();
//            fields.forEach(System.out::println);

//
//            Descriptors.FieldDescriptor fieldDescriptor = value.getDescriptorForType().findFieldByName("protobuftest.Person.id");
//            System.out.println(value.getField(fieldDescriptor));


            // manually write here!

            // 1 - take repeated Person
            recordConsumer.startField("p", 0);
            Descriptors.FieldDescriptor peopleId = value.getDescriptorForType().findFieldByName("people");
            Object people = value.getField(peopleId);

            List<?> list = (List<?>) people;
            for (Object listEntry: list) {
                recordConsumer.startGroup();

                // Here, we'd manually map any fields that we need
                T e = (T) listEntry;
                Descriptors.FieldDescriptor id = e.getDescriptorForType().findFieldByName("id");
                Object fieldValue = e.getField(id);
                if (!Objects.isNull(fieldValue)) {
                    recordConsumer.startField("mapped_id", 0);
                    recordConsumer.addInteger((Integer) fieldValue);
                    recordConsumer.endField("mapped_id", 0);
                }
                recordConsumer.endGroup();
            }

            recordConsumer.endField("p", 0);
        }
//
//        // Need to map to specific parquet index
//        class FieldWriter {
//            String fieldName;
//            int index = -1;
//
//            void setFieldName(String fieldName) {
//                this.fieldName = fieldName;
//            }
//
//            /** sets index of field inside parquet message.*/
//            void setIndex(int index) {
//                this.index = index;
//            }
//
//            /** Used for writing repeated fields*/
//            void writeRawValue(Object value) {
//
//            }
//
//            /** Used for writing nonrepeated (optional, required) fields*/
//            void writeField(Object value) {
//                recordConsumer.startField(fieldName, index);
//                writeRawValue(value);
//                recordConsumer.endField(fieldName, index);
//            }
//        }
    }
//
//    // Wire how the writing works
//    class MessageWriter extends CustomFieldWriter {
//
//        final CustomFieldWriter[] fieldWriters;
//
//        @SuppressWarnings("unchecked")
//        MessageWriter(Descriptors.Descriptor descriptor, GroupType schema) {
//            List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
//            fieldWriters = (CustomFieldWriter[]) Array.newInstance(CustomFieldWriter.class, fields.size());
//
//            for (Descriptors.FieldDescriptor fieldDescriptor : fields) {
//                String name = fieldDescriptor.getName();
//                try {
//                    Type type = schema.getType(name);
//                    CustomFieldWriter writer = createWriter(fieldDescriptor, type);
//
//                    if (fieldDescriptor.isRepeated()) {
//                        writer = new ArrayWriter(writer);
//                    }
//
//                    writer.setFieldName(name);
//                    writer.setIndex(schema.getFieldIndex(name));
//
//                    fieldWriters[fieldDescriptor.getIndex()] = writer;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//            System.out.println("init MessageWriter");
//        }
//
//        private CustomFieldWriter createWriter(Descriptors.FieldDescriptor fieldDescriptor, Type type) {
//
//            switch (fieldDescriptor.getJavaType()) {
//                case STRING:
//                    return new StringWriter();
//                case MESSAGE:
//                    return new MessageWriter(fieldDescriptor.getMessageType(), type.asGroupType());
//                case INT:
//                    return new IntWriter();
//                case LONG:
//                    return new LongWriter();
//                case FLOAT:
//                    return new FloatWriter();
//                case DOUBLE:
//                    return new DoubleWriter();
//                case ENUM:
//                    return new EnumWriter();
//                case BOOLEAN:
//                    return new BooleanWriter();
//                case BYTE_STRING:
//                    return new BinaryWriter();
//            }
//
//            return unknownType(fieldDescriptor);//should not be executed, always throws exception.
//        }
//
//
//
//        /** Writes top level message. It cannot call startGroup() */
//        void writeTopLevelMessage(Object value) {
//            writeAllFields((MessageOrBuilder) value);
//        }
//
//        /** Writes message as part of repeated field. It cannot start field*/
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.startGroup();
//            writeAllFields((MessageOrBuilder) value);
//            recordConsumer.endGroup();
//        }
//
//        /** Used for writing nonrepeated (optional, required) fields*/
//        @Override
//        final void writeField(Object value) {
//            recordConsumer.startField(fieldName, index);
//            recordConsumer.startGroup();
//            writeAllFields((MessageOrBuilder) value);
//            recordConsumer.endGroup();
//            recordConsumer.endField(fieldName, index);
//        }
//
//        private void writeAllFields(MessageOrBuilder pb) {
//            //returns changed fields with values. Map is ordered by id.
//            Map<Descriptors.FieldDescriptor, Object> changedPbFields = pb.getAllFields();
//
//            for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : changedPbFields.entrySet()) {
//                Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
//
//                if(fieldDescriptor.isExtension()) {
//                    // Field index of an extension field might overlap with a base field.
//                    throw new UnsupportedOperationException(
//                            "Cannot convert Protobuf message with extension field(s)");
//                }
//
//                int fieldIndex = fieldDescriptor.getIndex();
//                fieldWriters[fieldIndex].writeField(entry.getValue());
//            }
//        }
//    }
//
//    class ArrayWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        final CustomProtoWriteSupport.CustomFieldWriter fieldWriter;
//
//        ArrayWriter(CustomProtoWriteSupport.CustomFieldWriter fieldWriter) {
//            this.fieldWriter = fieldWriter;
//        }
//
//        @Override
//        final void writeRawValue(Object value) {
//            throw new UnsupportedOperationException("Array has no raw value");
//        }
//
//        @Override
//        final void writeField(Object value) {
//            recordConsumer.startField(fieldName, index);
//            List<?> list = (List<?>) value;
//
//            for (Object listEntry: list) {
//                fieldWriter.writeRawValue(listEntry);
//            }
//
//            recordConsumer.endField(fieldName, index);
//        }
//    }
//
//    /** validates mapping between protobuffer fields and parquet fields.*/
//    private void validatedMapping(Descriptors.Descriptor descriptor, GroupType parquetSchema) {
//        List<Descriptors.FieldDescriptor> allFields = descriptor.getFields();
//
//        for (Descriptors.FieldDescriptor fieldDescriptor: allFields) {
//            String fieldName = fieldDescriptor.getName();
//            int fieldIndex = fieldDescriptor.getIndex();
//            int parquetIndex = parquetSchema.getFieldIndex(fieldName);
//            if (fieldIndex != parquetIndex) {
//                String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
//                throw new IncompatibleSchemaModificationException(message);
//            }
//        }
//    }
//
//
//    class StringWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            Binary binaryString = Binary.fromString((String) value);
//            recordConsumer.addBinary(binaryString);
//        }
//    }
//
//    class IntWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.addInteger((Integer) value);
//        }
//    }
//
//    class LongWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.addLong((Long) value);
//        }
//    }
//
//    class FloatWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.addFloat((Float) value);
//        }
//    }
//
//    class DoubleWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.addDouble((Double) value);
//        }
//    }
//
//    class EnumWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            Binary binary = Binary.fromString(((Descriptors.EnumValueDescriptor) value).getName());
//            recordConsumer.addBinary(binary);
//        }
//    }
//
//    class BooleanWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            recordConsumer.addBoolean((Boolean) value);
//        }
//    }
//
//    class BinaryWriter extends CustomProtoWriteSupport.CustomFieldWriter {
//        @Override
//        final void writeRawValue(Object value) {
//            ByteString byteString = (ByteString) value;
//            Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
//            recordConsumer.addBinary(binary);
//        }
//    }
//
//    private CustomProtoWriteSupport.CustomFieldWriter unknownType(Descriptors.FieldDescriptor fieldDescriptor) {
//        String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
//                + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
//        throw new InvalidRecordException(exceptionMsg);
//    }
//
    /** Returns message descriptor as JSON String*/
    private String serializeDescriptor(Class<? extends Message> protoClass) {
        Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
        DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
        return TextFormat.printToString(asProto);
    }
}
