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
    @Override
    public OffsetDateTime decode(BsonReader bsonReader, DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(BsonWriter bsonWriter, OffsetDateTime offsetDateTime, EncoderContext encoderContext) {
        final Document document = new Document();
        document.put(DATE_FIELD, Date.from(offsetDateTime.toInstant()));
        document.put(OFFSET_FIELD, offsetDateTime.getOffset().toString());
        bsonWriter.writeStartDocument();
        bsonWriter.writeDateTime(DATE_FIELD, offsetDateTime.toInstant().toEpochMilli());
        bsonWriter.writeString(OFFSET_FIELD, offsetDateTime.getOffset().toString());
        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<OffsetDateTime> getEncoderClass() {
        return OffsetDateTime.class;
    }
}
