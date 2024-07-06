/******************************************************************************/


import { success, fail } from '#requests/common';

import { getDBResult } from '#db/common';
import { dbBkpGetTableList } from '#db/backup';

import { r2KeyExists, r2FetchJSON } from '#r2';


/******************************************************************************/


/* Given the key prefix for the R2 keys used during a DB restore, the database
 * to restore into, and a table entry that specifies the details of that
 * particular table, create the table and its indexes, then insert all of the
 * data within it.
 *
 * The return value is an object that contains the name of the table, the number
 * of indexes it has, and the number of rows inserted.
 *
 * If the file containing the table data is not found in the R2 bucket, this
 * will return null instead. */
async function restoreTable(ctx, toDb, keyPrefix, table) {
  // Determine the key for the file that would contain the contents for this
  // particular table, then fetch the object out; if we can't it then we can
  // return right away.
  const key = `${keyPrefix}/${table.name}.json`;
  const data = await r2FetchJSON(ctx, key);
  if (data === null) {
    return null;
  }

  // Generate a SQL batch to create the table and all of its indexes and
  // execute it.
  const DDL = [table.sql, ...table.indexes.map(i => i.sql)];
  const createBatch = DDL.map(sql => toDb.prepare(sql));
  getDBResult('restoreTable', table.name, await toDb.batch(createBatch));

  const result = {
    name: table.name,
    indexes: table.indexes.length,
    rows: data.length
  }

  // If there is no data to insert, then we can go ahead and return right now
  // since there is nothing else to do.
  if (data.length === 0) {
    return result;
  }

  // Prepare a statement that will insert data into this table
  const insert = toDb.prepare(`
    INSERT INTO ${table.name}
             (${table.columns.join(', ')})
      VALUES (${Array(table.columns.length).fill('?').join(', ')})`
  );

  // Create a batch insert that will insert all of the data for the table into
  // the database.
  const insertBatch = data.map(e => insert.bind(...e));
  await toDb.batch(insertBatch);

  // Return the result object back.
  return result;
}


/******************************************************************************/


/* Handles a request for a DB restore; this requires a combination of the
 * original database name and the backup name in order to be able to locate the
 * required file(s) to do the restore, and the name of the database to insert
 * the new data into.
 *
 * The data will be streamed from the bucket and inserted into the destination
 * database. */
export async function reqRestoreDump(ctx) {
  // Grab the database and backup names out of the body, then use the source DB
  // name to generate the key prefix to use for all files in the R2 bucket.
  const { fromDatabase, toDatabase, name } = ctx.req.valid('json');
  const keyPrefix = `${fromDatabase}/${name}`;

  // Pull the destination database binding out of the context; if this does not
  // match a know DB, generate an error.
  const toDb = ctx.env[toDatabase];
  if (toDb === undefined) {
    return fail(ctx, `no such bound database '${toDatabase}'`);
  }

  // 1. Try to fetch the metadata file; if this doesnt work, then we can't
  //    actually restore anything because the main control file is missing.
  const metaKey = `${keyPrefix}/metadata.json`;
  const metadata = await r2FetchJSON(ctx, metaKey);
  if (metadata === null) {
    return fail(ctx, `metadata file '${metaKey}' not found`);
  }

  // 2. Verify that none of the tables mentioned in the metadata exist; if they
  //    do, then we're gonna be trying to insert data into the database when it
  //    is already present.
  let currentData = await dbBkpGetTableList(toDb);
  const existing = Object.keys(currentData).filter(e => metadata.tables[e] !== undefined);
  if (existing.length !== 0) {
    return fail(ctx, `cannot restore; tables to be restored already exist`, 400, existing);
  }

  // 3. Verify that all of the data files that should be present actually are
  //    present before we start; if any are missing, then we can't restore, so
  //    there is no need to try.
  const assets = await Promise.allSettled(
    metadata.loadOrder.map(async (name) => {
      const assetKey = `${keyPrefix}/${name}.json`;
      const metadata = await r2KeyExists(ctx, assetKey);
      return {name, metadata };
    })
  );

  // Filter down the list of assets to just the names of assets whose data files
  // were not found.
  const missing = assets.filter(e => e.value.metadata === null).map(e => e.value.name);
  if (missing.length !== 0) {
    return fail(ctx, `cannot restore; table data files are missing`, 400, missing);
  }

  // Construct what will be our eventual return value.
  const result = {
    fromDatabase,
    toDatabase,
    name,
    baseKey: keyPrefix,
    tables: []
  }

  // 4. Going in table load order
  //    a. Create the table
  //    b. Create the indexes
  //    c. Insert all of the rows
  //
  // On any failure, leave the database in the last known state for the user to
  // clean up themselves; there may be issues that need to be investigated, and
  // it's a disaster waiting to happen if any code drops tables, even at the
  // behest of the user.
  for (const name of metadata.loadOrder) {
    const table = metadata.tables[name];
    const record = await restoreTable(ctx, toDb, keyPrefix, table);

    if (record === null) {
      return fail(ctx, `unable to restore backup; error in ${name}`);
    }

    // All good; add this as a restored table.
    result.tables.push(record);
  };

  return success(ctx, 'backup restored', result);
}


/******************************************************************************/
