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
		final List<KustoResultColumnPopulator<R, ?, ?>> queryResultColumns = new ArrayList<>();
		final Supplier<R> objConstructor;
		
		public Builder(Supplier<R> objConstructor) {
			this.objConstructor = objConstructor;
		}
		
		/**
		 * Add a column by name. Using this function will cause the KustoResultMapper to lookup the column by name once per column per call to
		 * {@link KustoResultMapper##extractList(KustoResultSetTable)} or {@link KustoResultMapper##extractSingle(KustoResultSetTable)}
		 * 
		 * @param <C>
		 *            Java type returned by the column (based on the KustoType parameter)
		 * @param type
		 *            {@link KustoType} returned by the column
		 * @param name
		 *            column name
		 * @param isNullable
		 *            whether the column may contains null values
		 * @param setter
		 *            function for setting a cell value into a pojo instance
		 * @return
		 */
		public <C> Builder<R> addColumn(KustoType<C> type, String name, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(KustoResultColumnPopulator.of(name, type, isNullable, setter));
			return this;
		}
		
		/**
		 * Add a column by name and ordinal (column index). The ordinal value will be preferred for extracting values from the
		 * {@link KustoResultSetTable}.
		 * 
		 * @param <C>
		 *            Java type returned by the column (based on the KustoType parameter)
		 * @param type
		 *            {@link KustoType} returned by the column
		 * @param name
		 *            column name
		 * @param ordinal
		 *            index of the column in the
		 * @param isNullable
		 *            whether the column may contains null values
		 * @param setter
		 *            function for setting a cell value into a pojo instance
		 * @return
		 */
		public <C> Builder<R> addColumn(KustoType<C> type, String name, int ordinal, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(KustoResultColumnPopulator.of(name, ordinal, type, isNullable, setter));
			return this;
		}
		
		/**
		 * Add a column by ordinal (column index).
		 * 
		 * @param <C>
		 *            Java type returned by the column (based on the KustoType parameter)
		 * @param type
		 *            {@link KustoType} returned by the column
		 * @param ordinal
		 *            index of the column in the
		 * @param isNullable
		 *            whether the column may contains null values
		 * @param setter
		 *            function for setting a cell value into a pojo instance
		 * @return
		 */
		public <C> Builder<R> addColumn(KustoType<C> type, int ordinal, boolean isNullable, BiConsumer<R, C> setter) {
			this.queryResultColumns.add(KustoResultColumnPopulator.of(ordinal, type, isNullable, setter));
			return this;
		}
		
		public KustoResultMapper<R> build() {
			return new KustoResultMapper<>(this.queryResultColumns, this.objConstructor);
		}
	}
	
	public static <R> Builder<R> newBuilder(Supplier<R> objConstructor) {
		return new Builder<>(objConstructor);
	}
	
	final List<KustoResultColumnPopulator<R, ?, ?>> columns;
	final Supplier<R> objConstructor;
	
	private KustoResultMapper(List<KustoResultColumnPopulator<R, ?, ?>> columns, Supplier<R> objConstructor) {
		this.columns = columns;
		this.objConstructor = objConstructor;
	}
	
	public R extractSingle(KustoResultSetTable resultSet) {
		R ret = null;
		if (resultSet.next()) {
			ret = this.objConstructor.get();
			for (KustoResultColumnPopulator<R, ?, ?> col : this.columns) {
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
				KustoResultColumnPopulator<R, ?, ?> col = this.columns.get(i);
				col.populateFrom(r, resultSet, columnOrdinals[i]);
			}
			ret.add(r);
		}
		return ret;
	}
}
