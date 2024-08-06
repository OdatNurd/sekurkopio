/******************************************************************************/


import { getDBResult } from '#db/common';


/******************************************************************************/


/* Fetch from the database a list of all of the tables that currently exist, as
 * well as the indexes that are on each.
 *
 * The return value is a table which contains keys that are the names of the
 * defined tables, and objects of the form:
 *     {
 *         "type": "table",
 *         "name": "NameOfTable",
 *         "sql": "-- Table Definition SQL",
 *         "indexes": [
 *             {
 *                 "type": "index",
 *                 "name": "NameOfIndex",
 *                 "sql": "-- Index Definition SQL"
 *             }
 *         ]
 *     }
 *
 * The keys representing table names are the name that is reported by SQLite,
 * which takes them directly from the SQL that was used to define the table. */
export async function dbBkpGetTableList(db) {
  // Fetch from the DB the complete list of all non-SQLite and non-CF tables
  // along with their indexes.
  const rawLookup = await db.prepare(`
    SELECT type, name, tbl_name, sql FROM sqlite_master
     WHERE type in ('table', 'index')
       AND name NOT LIKE "sqlite_%"
       AND name NOT LIKE "_cf_%"
     ORDER BY tbl_name ASC
  `).all();

  // Pull the data out of the query
  const rawData = getDBResult('dbBkpGetTableList', 'get_tbl_info', rawLookup);

  // The results of the query mix tables and indexes together; split them apart
  // for easier access.
  const tables = rawData.filter(e => e.type === 'table');
  const indexes = rawData.filter(e => e.type === 'index');

  // For each of the tables that exist in the list, add them to a table list
  // object.
  const tableList = tables.reduce((result, table) => {
    // Remove the redundant table name from the table object and add in key
    // to track the indexes (there may not be any).
    delete table.tbl_name;
    table.indexes = [];

    result[table.name] = table;
    return result;
  }, {});

  // Now scan over the indexes and populate them into their associated table.
  //
  // SQLite ensures that when it reports the table name an index is on that it
  // is the same case as the table definition described above, even if the index
  // SQL definition specifies the table name in a different case.
  indexes.forEach(index => {
    tableList[index.tbl_name].indexes.push(index);
    delete index.tbl_name;
  });

  return tableList;
}


/******************************************************************************/


/* Given a list of tables of the form returned by dbBkpGetTableList(), make a
 * clone of the incoming value, fetch the list of column names and foreign key
 * constraints for each table, and add them to the appropriate table in the
 * object.
 *
 * Columns are added in the form of an array of strings that represents the
 * columns in the table, in the order the SQL defined them:
 *
 *     "columns": ["column1", "column2"]
 *
 * Foreign keys are defined as an array of objects which indicate what table the
 * foreign key is in, and which column each side of the relation is:
 *
 *     "constraints": [
 *         {
 *             "table": "ForeignTableName",
 *             "from": "localColumnName",
 *             "to": "foreignColumnName",
 *         }
 *     ]
 *
 * In addition to the above, the unique list of table names from the above are
 * added to a key named "dependencies" with a reference to the table itself;
 * this key will be an empty object if a table has no foreign key constraints:
 *
 *     "dependencies": {
 *         "DependantTableName": { ... }
 *     }
 *
 * Since the values in the "dependencies" key are references to the objects from
 * the global table list, this makes it possible to completely walk the
 * constraint hierarchy on any given table.
 *
 * The returned value is a clone of the provided input table list (i.e. it is
 * not modified in place). Note also that the references to dependent tables
 * used in the dependencies object are references to the tables in the clone,
 * not to the passed in input. */
async function dbBkpPopulateTableDetails(db, inputTableList) {
  // Clone the incoming object so we can modify it safely.
  const tableList = structuredClone(inputTableList);

  // If the object is empty, there are no tables and thus we don't need to try
  // to capture any details.
  if (Object.keys(tableList).length === 0) {
    console.log(`database has no tables; no details to gather`);
    return tableList;
  }

  // Prepare queries to fetch the columns for a table and the list of foreign
  // key constraints.
  const columnsStmt = db.prepare(`SELECT name FROM pragma_table_info(?1)`);
  const foreignKeyStmt = db.prepare(`SELECT "table", "from", "to" FROM pragma_foreign_key_list(?1)`);

  // The table names for foreign keys come from the SQL that defined them, which
  // may not have the same case as the table name we know. To help with this,
  // create a mapping table that maps an uppercase version of the table name to
  // the name that we have, so we can associate things properly.
  const tableNames = Object.keys(tableList);
  const tableNameMap = tableNames.reduce((result, name) => {
    result[name.toUpperCase()] = name;
    return result;
  }, {});

  // Create a batch query that will look up the column names and foreign keys
  // in a specific table; the batch results will give us pairs of results for
  // each table.
  const batch = [];
  tableNames.forEach(name => {
    batch.push(columnsStmt.bind(name));
    batch.push(foreignKeyStmt.bind(name));
  });

  // Execute the batch; the result set will be 2 arrays per table, in the order
  // of tables from the table names list.
  const rawMetadata = await db.batch(batch);
  const metadata = getDBResult('dbBkpPopulateTableDetails', 'get_tbl_meta', rawMetadata);

  // Walk the list of table names, and for each one fill out the details from
  // the metadata.
  tableNames.forEach(table => {
    // Pull out the results for this table.
    const columns = metadata.shift();
    const constraints = metadata.shift();

    // Store the columns and constraints for this table. For constraints we need
    // to ensure that the table that is named has the same case as the main
    // table list, since SQLite reports constrained table names with the case of
    // the table used in the key definition.
    tableList[table].columns = columns.map(e => e.name);
    tableList[table].constraints = constraints.map(e => {
      // Look up a case normalized version of the name; if it's not what was
      // specified, patch the entry and generate a warning.
      const tableName = tableNameMap[e.table.toUpperCase()];
      if (tableName !== e.table) {
        console.log(`case-mismatched foreign key constraint: ${table}.${e.from} => ${e.table}.${e.to}`);
        e.table = tableName;
      }

      return e;
    });

    // Now, using the constraint information, determine the list of dependent
    // tables; this is an object with keys that are the names of tables this
    // table refers to, with the values of those keys being a reference to the
    // table itself; this makes the constraint table fully recursive.
    tableList[table].dependencies = constraints.reduce((result, e) => {
      // If we have already mapped this entry in, we don't need to do it again.
      if (result[e.table] !== undefined) {
        return result;
      }

      // Store a reference to the table into the object; this is not error
      // checked because our dump contains all tables, and so any foreign keys
      // must be on tables that actually exist.
      result[e.table] = tableList[e.table];
      return result;
    }, {})
  });

  return tableList;
}


/******************************************************************************/


/* This function takes as input an object whose keys are the names of tables in
 * the database and whose values represent the details of those tables, and uses
 * the information provided to update the provided output load order such that
 * it contains the order in which the tables should be inserted into the DB to
 * not cause any constraint violations.
 *
 * The function calls itself recursively in a depth first search in order to
 * root the load order in the left nodes that have no constraints, before
 * considering the tables that rely on those constraints.
 *
 * Note that the resulting insertion order is not guaranteed to be optimal, just
 * one that will not violate constraints by ensuring that all tables that any
 * particular table depends on is inserted first. */
function getTableLoadOrder(node, outLoadOrder=undefined, depth=0) {
  outLoadOrder ??= [];

  // Handle every value within the node we were given
  for (const table of Object.values(node)) {
    // Our constraints table is an object similar to this node; recursively call
    // back into ourselves with that list.
    getTableLoadOrder(table.dependencies, outLoadOrder, depth + 1);

    // If we have not already been visited as a part of this search, add ourselves
    // to the end of the load order and mark this as a visit.
    //
    // We might appear several times in the traversal, but we only need to
    // record ourselves once.
    if (table.visited !== true) {
      table.visited = true;
      outLoadOrder.push(table.name);
    }
  }

  // If we're about to return back from the outer call, trim the node to remove
  // the flags we placed there; this can't happen during the traversal since we
  // need to know when we've visited everything.
  if (depth === 0) {
    Object.values(node).forEach(table => delete table.visited);
  }

  return outLoadOrder;
}


/******************************************************************************/


/* Given a single tableInfo record from one of the key values returned by
 * dbBkpGetTableList(), gather all of the data from that table and return it
 * back.
 *
 * The returned data is in the form of an array of array of values, where each
 * sub-array has all of the values of the columns of the table, in order.
 *
 * Note that this uses a raw() request for speed and space savings, but such a
 * request does not return metadata like standard D1 queries; in order to log
 * a semi-consistent return, this generates fake meta info so that details can
 * be logged in a consistent manner. */
export async function dbBkpGetTableContents(db, tableInfo) {
  // Generate a query that will return back all of the data for the given table;
  // this will query only the non-generated columns as defined in the table
  // definition, and they are also queried in that specific order.
  const results = await db.prepare(`
    SELECT ${tableInfo.columns.join(', ')} FROM ${tableInfo.name};
  `).raw();

  // The raw request does not return meta information back, so we need to gin up
  // our own.
  return getDBResult('dbBkpGetTableContents', 'get_tbl_data', {
    success: 1,
    meta: {
      last_row_id: 0,
      rows_read: results.length,
      served_by: 'fake-meta-log', // 'miniflare', 'v3-prod', etc
      rows_written: 0
    },
    results
  });
}


/******************************************************************************/


/* Examine the database in the provided context and find:
 *   1. All of the tables that are not SQLite system tables of CF Special Tables
 *   2. All indexes and foreign key constraints on each of those tables
 *   3. The DDL for each table and index
 *   4. The full data for each table found
 *
 * Return back a JSON object of the form:
 *     {
 *         "loadOrder": ["Table1", "Table2"],
 *         "tables": {
 *             "NameOfTable": {
 *                 "type": "table",
 *                 "name": "NameOfTable",
 *                 "sql": "-- Table Definition SQL",
 *                 "indexes": [
 *                     {
 *                         "type": "index",
 *                         "name": "NameOfIndex",
 *                         "sql": "-- Index Definition SQL"
 *                     }
 *                 ],
 *                 "columns": ["column1", "column2"],
 *                 "constraints": [
 *                     {
 *                         "table": "ForeignTableName",
 *                         "from": "localColumnName",
 *                         "to": "foreignColumnName",
 *                     }
 *                 ],
 *                 "data": [
 *                     [ ... ],
 *                     [ ... ],
 *                 ]
 *             }
 *             "Table2": { ... }
 *         }
 *     }
 *
 * Each entry in the "tables" key represents a table and all of the information
 * about it. The "loadOrder" specifies in what order the tables should be added
 * to a blank database in order to not encounter any constraint violations on
 * foreign key constraints.
 *
 * Note that the load order does not guarantee that all tables with no
 * constraints appear first, only that the order will not cause any violations
 * on insert. */
export async function dbBkpGenerateMetaInfo(db) {
  // Get the list of tables in the database, and then populate in the inner
  // details on the names and orders of the columns as well as the list of
  // foreign key constraints.
  let tables = await dbBkpGetTableList(db);
  tables = await dbBkpPopulateTableDetails(db, tables);

  // Determine the proper order in which the tables need to be loaded into the
  // database in order for us to not violate any constraints on the data.
  const loadOrder = getTableLoadOrder(tables);

  // For each table, remove the dependencies since we no longer need them.
  Object.values(tables).forEach(table => delete table.dependencies);

  return { loadOrder, tables };
}


/******************************************************************************/


/* Fetch from the database a complete list of all of the known backups.
 *
 * The return value is a (potentially empty) list of objects that indicate what
 * database the backup is for and what the backup name is.
 *
 * Backup names will have a `.tar` or `.tgz` extension if they're tar backups;
 * otherwise they are regular directory based file backups. */
export async function dbBkpGetList(db) {
  // Grab the list of backups from the database.
  const lookup = await db.prepare(`
    SELECT id, dbName, backupName from BackupList
  `).all();

  // Pull the data out of the query and return it
  return getDBResult('dbBkpGetList', 'get_backup_list', lookup);
}


/******************************************************************************/


/* Insert a record for a newly created backup into the tracking database.
 *
 * This will create a new record tracking that a backup with the given name
 * was created/updated for the provided database.
 *
 * The record for the backup is returned back; if a backup by this name for this
 * database already existed, then the existing record will be returned back
 * instead of doing an insertion, since any such operation would overwrite all
 * of the files for that backup anyway. */
export async function dbBkpInsert(db, fromDatabase, name) {
  // Check to see if there is an existing backup with this name already exists.
  const existingQuery = await db.prepare(`
    SELECT id, dbName, backupName
      FROM BackupList
     WHERE dbName = ?1 AND backupName = ?2
  `).bind(fromDatabase, name).all();
  const existing = getDBResult('dbBkpInsert', 'check_existing', existingQuery);

  // If there is a record of this in the database, we don't need to do anything
  // and can just go ahead and return the value directly.
  if (existing.length !== 0) {
    console.log(`insert of new backup for ${fromDatabase}:${name} overwrote an existing backup`);
    return existing[0];
  }

  // There is no such record, so insert a new one into the database.
  const result = await db.prepare(`
    INSERT INTO BackupList
      (dbName, backupName)
    VALUES (?1, ?2);
  `).bind(fromDatabase, name).all();

  // Display the results of the creation
  getDBResult('dbBkpInsert', 'insert_backup', result);

  // Return the new record back; all data here is known except for the ID, which
  // comes from the insert.
  return {
    id: result.meta.last_row_id,
    dbName: fromDatabase,
    backupName: name
  }
}


/******************************************************************************/
