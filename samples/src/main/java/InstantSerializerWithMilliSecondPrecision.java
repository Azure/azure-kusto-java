import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import java.time.format.DateTimeFormatterBuilder;

public class InstantSerializerWithMilliSecondPrecision extends InstantSerializer {

    public InstantSerializerWithMilliSecondPrecision() {
        super(InstantSerializer.INSTANCE, false, new DateTimeFormatterBuilder().appendInstant(3).toFormatter());
    }
}