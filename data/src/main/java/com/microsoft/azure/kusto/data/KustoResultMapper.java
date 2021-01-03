package com.microsoft.azure.kusto.data;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A class for mapping Kusto results to a list of pojos
 * 
 * @param <R>
 *            pojo type returned by the mapping
 */
public class KustoResultMapper<R> {
	
	public static class Builder<R> {
		final List<ObjectPopulator<R, ?, ?>>	queryResultColumns	= new ArrayList<>();
		final Supplier<R>									objConstructor;
		
		public Builder(Supplier<R> objConstructor) {
			this.objConstructor = objConstructor;
		}
		
		<C> Builder<R> addColumn(KustoType<C> type, String name, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(ObjectPopulator.of(name, type, isNullable, setter));
			return this;
		}
		
		<C> Builder<R> addColumn(KustoType<C> type, String name, int ordinal, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(ObjectPopulator.of(name, ordinal, type, isNullable, setter));
			return this;
		}
		
		<C> Builder<R> addColumn(KustoType<C> type, int ordinal, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(ObjectPopulator.of(ordinal, type, isNullable, setter));
			return this;
		}
		
		public <C> Builder<R> addNonNullableColumn(KustoType<C> type, String name, BiConsumer<R, C> setter) {
			return addColumn(type, name, false, setter);
		}
		
		public <C> Builder<R> addNonNullableColumn(KustoType<C> type, int ordinal, BiConsumer<R, C> setter) {
			return addColumn(type, ordinal, false, setter);
		}
		
		public <C> Builder<R> addNonNullableColumn(KustoType<C> type, String name, int ordinal, BiConsumer<R, C> setter) {
			return addColumn(type, name, ordinal, false, setter);
		}
		
		public <C> Builder<R> addNullableColumn(KustoType<C> type, String name, BiConsumer<R, C> setter) {
			return addColumn(type, name, true, setter);
		}
		
		public <C> Builder<R> addNullableColumn(KustoType<C> type, String name, int ordinal, BiConsumer<R, C> setter) {
			return addColumn(type, name, ordinal, true, setter);
		}
		
		public <C> Builder<R> addNullableColumn(KustoType<C> type, int ordinal, BiConsumer<R, C> setter) {
			return addColumn(type, ordinal, true, setter);
		}
		
		public KustoResultMapper<R> build() {
			return new KustoResultMapper<>(this.queryResultColumns, this.objConstructor);
		}
	}
	
	public static <R> Builder<R> newBuilder(Supplier<R> objConstructor) {
		return new Builder<>(objConstructor);
	}
	
	final List<ObjectPopulator<R, ?, ?>>	columns;
	final Supplier<R>									objConstructor;
	
	private KustoResultMapper(List<ObjectPopulator<R, ?, ?>> columns, Supplier<R> objConstructor) {
		this.columns = columns;
		this.objConstructor = objConstructor;
	}
	
	public R extractSingle(KustoResultSetTable resultSet) {
		R ret = this.objConstructor.get();
		if (resultSet.next()) {
			for (ObjectPopulator<R, ?, ?> col : this.columns) {
				col.populateFrom(ret, resultSet);
			}
		}
		return ret;
	}
	
	public List<R> extractList(KustoResultSetTable resultSet) {
		List<R> ret = new ArrayList<>(resultSet.count());
		int[] columnOrdinals = new int[this.columns.size()];
		for (int i = 0; i < this.columns.size(); i++) {
			columnOrdinals[i] = this.columns.get(i).columnIndexInResultSet(resultSet);
		}
		while (resultSet.next()) {
			R r = this.objConstructor.get();
			for (int i = 0; i < this.columns.size(); i++) {
				ObjectPopulator<R, ?, ?> col = this.columns.get(i);
				col.populateFrom(r, resultSet, columnOrdinals[i]);
			}
			ret.add(r);
		}
		return ret;
	}
}
