package com.microsoft.azure.kusto.data;

/**
 * Represents a single Kusto result column with a type, name and nullability information
 * 
 * @param <C>
 *            the Java class returned by the ADX column type
 * @param <T>
 *            type of ADX column
 */
public class KustoColumn<C, T extends KustoType<C>> {
	final String	name;
	final T			type;
	final boolean	isNullable;
	
	static <C, T extends KustoType<C>> KustoColumn<C, T> of(String name, T type, boolean isNullable) {
		return new KustoColumn<>(name, type, isNullable);
	}
	
	static <C, T extends KustoType<C>> KustoColumn<C, T> ofNotNullable(String name, T type) {
		return new KustoColumn<>(name, type, false);
	}
	
	static <C, T extends KustoType<C>> KustoColumn<C, T> ofNullable(String name, T type) {
		return new KustoColumn<>(name, type, true);
	}
	
	KustoColumn(String name, T type, boolean isNullable) {
		this.name = name;
		this.type = type;
		this.isNullable = isNullable;
	}
	
	C extractFrom(KustoResultSetTable resultSet) {
		Object o = resultSet.getObject(this.name);
		if (o == null) {
			if (!this.isNullable) {
				throw new NullPointerException("Column " + this.name + " is not nullable");
			}
			return null;
		}
		try {
			return this.type.type(o);
		} catch (Exception e) {
			throw new IllegalArgumentException("Column " + this.name + " is of type " + o.getClass() + " but I expected type " + this.type.clazz, e);
		}
	}
}
