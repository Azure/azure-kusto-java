package com.microsoft.azure.kusto.data;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Function;

/**
 * Represents various ADX data types and their bindings to Java classes
 * 
 * @author alex
 *
 * @see https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/
 *
 * @param <C>
 */
public class KustoType<C> {
	
	public static final KustoType<Integer>			INTEGER						= new KustoType<>(Integer.class);
	// Longs often return as "Integer", so we need to cast to "number", and then get the long value:
	public static final KustoType<Long>				LONG						= new KustoType<>(Long.class,
			o -> Long.valueOf(Number.class.cast(o).longValue()));
	public static final KustoType<UUID>				GUID_UUID					= new KustoType<>(UUID.class, o -> UUID.fromString(o.toString()));
	public static final KustoType<String>			GUID_STRING					= new KustoType<>(String.class);
	public static final KustoType<String>			STRING						= new KustoType<>(String.class);
	public static final KustoType<Double>			REAL_DOUBLE					= new KustoType<>(Double.class);
	public static final KustoType<Float>			REAL_FLOAT					= new KustoType<>(Float.class,
			o -> Float.valueOf(Double.class.cast(o).floatValue()));
	
	public static final KustoType<ZonedDateTime>	DATETIME_ZONED_DATE_TIME	= new KustoType<>(ZonedDateTime.class, o -> ZonedDateTime.parse(o.toString()));
	public static final KustoType<Instant>			DATETIME_INSTANT			= new KustoType<>(Instant.class, o -> Instant.parse(o.toString()));
	public static final KustoType<Long>				DATETIME_LONG				= new KustoType<>(Long.class, o -> Instant.parse(o.toString()).toEpochMilli());
	
	public static final KustoType<Object>			OBJECT						= new KustoType<>(Object.class);
	
	final Class<C>									clazz;
	final Function<Object, C>						typer;
	
	private KustoType(Class<C> clazz) {
		this(clazz, clazz::cast);
	}
	
	private KustoType(Class<C> clazz, Function<Object, C> typer) {
		this.clazz = clazz;
		this.typer = typer;
	}
	
	public C type(Object o) {
		return this.typer.apply(o);
	}
	
	public Class<C> getTypeClass() {
		return this.clazz;
	}
}
