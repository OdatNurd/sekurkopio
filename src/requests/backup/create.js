/******************************************************************************/


import { success, fail } from '#requests/common';

import { dbBkpGenerateMetaInfo, dbBkpGetTableContents, dbBkpInsert } from "#db/backup";

import { r2StoreJson } from '#r2';


/******************************************************************************/


/* Handles a request for a DB dump; this will generate a metadata JSON file that
 * describes all of the tables, plus one file per table to contain the data for
 * that table. All of the files will be sent to the R2 BACKUP bucket using a
 * key prefix that consists of the project name and a specified uniqueness key,
 * which defaults to the current date and time if not specified.
 *
 * The metadata contains information on all tables, how to define them, what
 * their indexes are, the names of their columns, as well as what order in which
 * to insert them in order to not violate any key constraints. */
export async function reqCreateDump(ctx) {
  // Grab the database and backup names out of the body, then use it to generate
  // the key prefix to use for all files in the R2 bucket.
  const { fromDatabase, name } = ctx.req.valid('json');
  const keyPrefix = `${fromDatabase}/${name}`;

  // Pull the database binding out of the context; if this does not match a
  // know DB, generate an error.
  const fromDb = ctx.env[fromDatabase];
  if (fromDb === undefined) {
    return fail(ctx, `no such bound database '${fromDatabase}'`);
  }

  // Grab the metadata that describes all of the tables and their relationships
  // with each other.
  const metadata = await dbBkpGenerateMetaInfo(fromDb);

  // Write the various files out now
  await r2StoreJson(ctx, `${keyPrefix}/metadata.json`, metadata);

  // Set up a basic result to return back.
  const result = {
    baseKey: keyPrefix,
    tables: []
  }

  // For each table in the load order list, fetch the data and then write it out
  // to a file in the DB.
  for (const tableName of metadata.loadOrder) {
    const table = metadata.tables[tableName];
    let data = await dbBkpGetTableContents(fromDb, table);

    // Add a record of data for this table to the eventual result.
    result.tables.push({
      name: table.name,
      indexes: table.indexes.length,
      rows: data.length,
      columns: table.columns,
    });

    // Store the table data into the bucket.
    await r2StoreJson(ctx, `${keyPrefix}/${tableName}.json`, data);
  }

  // If we get here, the backup succeeded, so make a record of it in the
  // database.
  await dbBkpInsert(ctx.env.sekurkopio, fromDatabase, name);

  // All Good.
  return success(ctx, `created a dump of ${metadata.loadOrder.length} tables`, result);
}


/******************************************************************************/
