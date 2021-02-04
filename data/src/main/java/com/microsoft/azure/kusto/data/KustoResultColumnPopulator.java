package com.microsoft.azure.kusto.data;

import java.util.function.BiConsumer;

/**
 * Represents a single Kusto result column and its mapping to a java object. Used to extract the column from a {@link KustoResultSetTable} and set the
 * value in a pojo. The column is defined by either its name or its ordinal (or both, in which case the ordinal is used).
 * 
 * @param <R>
 *            the pojo this column needs to set its value to
 * 			
 * @param <C>
 *            the Java class returned by the ADX column type
 * @param <T>
 *            type of ADX column
 */
class KustoResultColumnPopulator<R, C, T extends KustoType<C>> {
	
	final static int UNSET_ORDINAL = -1;
	
	final BiConsumer<R, C> valueSetter;
	final String name;
	final T type;
	final boolean isNullable;
	final int presetOrdinal;
	
	static <R, C, T extends KustoType<C>> KustoResultColumnPopulator<R, C, T> of(String name, int ordinal, T type, boolean isNullable,
			BiConsumer<R, C> valueSetter) {
		return new KustoResultColumnPopulator<>(name, ordinal, type, isNullable, valueSetter);
	}
	
	static <R, C, T extends KustoType<C>> KustoResultColumnPopulator<R, C, T> of(String name, T type, boolean isNullable, BiConsumer<R, C> valueSetter) {
		return new KustoResultColumnPopulator<>(name, UNSET_ORDINAL, type, isNullable, valueSetter);
	}
	
	static <R, C, T extends KustoType<C>> KustoResultColumnPopulator<R, C, T> of(int ordinal, T type, boolean isNullable, BiConsumer<R, C> valueSetter) {
		return new KustoResultColumnPopulator<>(null, ordinal, type, isNullable, valueSetter);
	}
	
	KustoResultColumnPopulator(String name, int ordinal, T type, boolean isNullable, BiConsumer<R, C> valueSetter) {
		this.name = name;
		this.presetOrdinal = ordinal;
		this.type = type;
		this.isNullable = isNullable;
		this.valueSetter = valueSetter;
	}
	
	void populateFrom(R objToPopulate, KustoResultSetTable resultSet) {
		populateFrom(objToPopulate, resultSet, this.presetOrdinal);
	}
	
	void populateFrom(R objToPopulate, KustoResultSetTable resultSet, int ordinal) {
		Object o = resultSet.getObject(columnIndexInResultSet(resultSet));
		if (o == null) {
			if (!this.isNullable) {
				throw new NullPointerException(String.format("Column %s (ordinal %d) is not nullable", this.name, ordinal));
			}
			this.valueSetter.accept(objToPopulate, null);
			return;
		}
		C typed;
		try {
			typed = this.type.type(o);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Column %s (ordinal %d) is of type %s but expected type is %s", this.name, ordinal,
					o.getClass().toString(), this.type.clazz.toString()), e);
		}
		this.valueSetter.accept(objToPopulate, typed);
	}
	
	int columnIndexInResultSet(KustoResultSetTable resultSet) {
		return this.presetOrdinal == UNSET_ORDINAL ? resultSet.findColumn(this.name) : this.presetOrdinal;
	}
}
