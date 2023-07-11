// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.mapping;

import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;

/**
 * Mapping to use upsert
 * 
 * @author gderuette
 * @param <E> mapped class
 */
public abstract class AbstractUpsertMapping<E> extends AbstractMapping<E> {

	private static Long idx = 0l;
	private final String primaryKey;
	private final String tableName;

	protected AbstractUpsertMapping(String schemaName, String tableName, String primaryKey) {
		this(new ValueHandlerProvider(), schemaName, tableName, false, primaryKey);
	}

	protected AbstractUpsertMapping(String schemaName, String tableName, boolean usePostgresQuoting,
			String primaryKey) {
		this(new ValueHandlerProvider(), schemaName, tableName, usePostgresQuoting, primaryKey);
	}

	protected AbstractUpsertMapping(IValueHandlerProvider provider, String schemaName, String tableName,
			boolean usePostgresQuoting, String primaryKey) {
		super(provider, schemaName, tableName + "_temp_" + getNextIdx(), usePostgresQuoting);
		this.primaryKey = primaryKey;
		this.tableName = tableName;
	}

	/**
	 * @return return current index for temp table. Used to prevent conflict on
	 *         table name
	 */
	private static synchronized Long getNextIdx() {
		return idx++;
	}

	/**
	 * @return merge primary key
	 */
	public String getPrimaryKey() {
		return this.primaryKey;
	}

	/**
	 * @return target table name
	 */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * @return temporary table name
	 */
	public String getTempTableName() {
		return this.table.getTableName();
	}
}
