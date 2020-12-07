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
	
	private static class QueryResultColumnInfo<R, C> {
		final KustoColumn<C, ? extends KustoType<C>>	type;
		final BiConsumer<R, C>						valueSetter;
		
		QueryResultColumnInfo(KustoColumn<C, ? extends KustoType<C>> col, BiConsumer<R, C> valueSetter) {
			this.type = col;
			this.valueSetter = valueSetter;
		}
		
		void extractFrom(R r, KustoResultSetTable resultSet) {
			this.valueSetter.accept(r, this.type.extractFrom(resultSet));
		}
	}
	
	public static class Builder<R> {
		final List<QueryResultColumnInfo<R, ?>>	queryResultColumns	= new ArrayList<>();
		final Supplier<R>						objConstructor;
		
		public Builder(Supplier<R> objConstructor) {
			this.objConstructor = objConstructor;
		}
		
		<C> Builder<R> addColumn(KustoType<C> type, String name, boolean isNullable, BiConsumer<R, C> setter) {
			KustoColumn<C, ? extends KustoType<C>> col = new KustoColumn<>(name, type, isNullable);
			this.queryResultColumns.add(new QueryResultColumnInfo<>(col, setter));
			return this;
		}
		
		public <C> Builder<R> addNonNullableColumn(KustoType<C> type, String name, BiConsumer<R, C> setter) {
			return addColumn(type, name, false, setter);
		}
		
		public <C> Builder<R> addNullableColumn(KustoType<C> type, String name, BiConsumer<R, C> setter) {
			return addColumn(type, name, true, setter);
		}
		
		public KustoResultMapper<R> build() {
			return new KustoResultMapper<>(this.queryResultColumns, this.objConstructor);
		}
	}
	
	public static <R> Builder<R> newBuilder(Supplier<R> objConstructor) {
		return new Builder<>(objConstructor);
	}
	
	final List<QueryResultColumnInfo<R, ?>>	columns;
	final Supplier<R>						objConstructor;
	
	private KustoResultMapper(List<QueryResultColumnInfo<R, ?>> columns, Supplier<R> objConstructor) {
		this.columns = columns;
		this.objConstructor = objConstructor;
	}
	
	public R extractSingle(KustoResultSetTable resultSet) {
		R ret = this.objConstructor.get();
		if (resultSet.next()) {
			for (QueryResultColumnInfo<R, ?> col : this.columns) {
				col.extractFrom(ret, resultSet);
			}
		}
		return ret;
	}
	
	public List<R> extractList(KustoResultSetTable resultSet) {
		List<R> ret = new ArrayList<>();
		while (resultSet.next()) {
			R r = this.objConstructor.get();
			for (QueryResultColumnInfo<R, ?> col : this.columns) {
				col.extractFrom(r, resultSet);
			}
			ret.add(r);
		}
		return ret;
	}
}
