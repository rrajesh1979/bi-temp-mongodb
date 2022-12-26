package org.rrajesh1979.bitemp.config;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.OffsetDateTime;
import java.util.Date;

public class OffsetDateTimeCodec implements Codec<OffsetDateTime> {
    public static final String DATE_FIELD = "dateTime";
    public static final String OFFSET_FIELD = "offset";
    public static final String EPOCH_MILLI = "epochMilli";

    @Override
    public OffsetDateTime decode(BsonReader bsonReader, DecoderContext decoderContext) {
        bsonReader.readStartDocument();
        Date date = new Date(bsonReader.readDateTime());
        String offset = bsonReader.readString();
        bsonReader.readEndDocument();
        return OffsetDateTime.ofInstant(date.toInstant(), OffsetDateTime.parse(offset).getOffset());
    }

    @Override
    public void encode(BsonWriter bsonWriter, OffsetDateTime offsetDateTime, EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();
        bsonWriter.writeDateTime(DATE_FIELD, offsetDateTime.toInstant().toEpochMilli());
        bsonWriter.writeString(OFFSET_FIELD, offsetDateTime.getOffset().toString());
        bsonWriter.writeInt64(EPOCH_MILLI, offsetDateTime.toInstant().toEpochMilli());
        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<OffsetDateTime> getEncoderClass() {
        return OffsetDateTime.class;
    }
}
