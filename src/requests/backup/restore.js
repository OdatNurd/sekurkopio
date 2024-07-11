/******************************************************************************/


import { success, fail } from '#requests/common';

import { getDBResult } from '#db/common';
import { dbBkpGetTableList } from '#db/backup';

import { r2KeyExists, r2FetchJSON, r2RawGet } from '#r2';

import { extract, TAR_OBJECT_TYPE_FILE } from 'streaming-tarball';


/******************************************************************************/


/* Given a key that represents a tarball, return back a stream that can be used
 * to extract the contents of the tarball.
 *
 * This can handle uncompressed keys (extension of '.tar') and gzipped tar files
 * (extention of '.tgz') only.
 *
 * If the key represents a file that exists in the bucket and is one of the
 * supported tar formats (based on filename), a stream is returned that can be
 * extracted by streaming-tarball ; otherwise null is returned. */
async function getTarStream(ctx, key) {
  // Verify that the file that this represents is one of the formats that we can
  // handle.
  if (key.endsWith('.tar') === false && key.endsWith('.tgz') === false) {
    console.log(`input key ${key} is not a recongnized tarball filename`);
    return null;
  }

  // Try to fetch the tarball object from the bucket; early fail if we can't.
  const tarball = await r2RawGet(ctx, key);
  if (tarball === null) {
    return null;
  }

  // If this is a compressed tarball, then we need to send the output through
  // a decompression stream.
  if (key.endsWith('.tgz')) {
    return tarball.body.pipeThrough(new DecompressionStream('gzip'));
  }

  // The tar archive is "bare".
  return tarball.body;
}


/******************************************************************************/


/* Given a database and a metadata object for doing a restore, verify that the
 * database does not contain any of the tables that are about to be restored.
 *
 * The return value is a list of all of the tables that exist in the database
 * already AND which are scheduled to be restored by the backup described by the
 * metadata.
 *
 * This list could be empty. */
async function verifyDbTables(ctx, db, metadata) {
  // Gather the list of tables from the database and filter that down to tables
  // that exist in the passed in metadata.
  let currentData = await dbBkpGetTableList(db);

  // Return the list of tables that exist in both places.
  return Object.keys(currentData).filter(e => metadata.tables[e] !== undefined);
}


/******************************************************************************/


/* Given a database to restore into, a table dictionary that describes a table
 * to be restored, and a data object that is a stream of the data to be inserted
 * into the table and will:
 *   1. Create the table in the database
 *   2. Create all indexes on the table
 *   3. Insert all of the data from the data stream into the table.
 *
 * The current implementation presumes that "await dataStream.text()" will
 * content of the file to be used to contain the data, and that it is stored in
 * a JSON format.
 *
 * The return value is an object that contains the name of the table, the number
 * of indexes it has, and the number of rows inserted. */
async function restoreTable(ctx, dbHandle, table, dataStream) {
  // Generate a SQL batch to create the table and all of its indexes and
  // execute it.
  const DDL = [table.sql, ...table.indexes.map(i => i.sql)];
  const createBatch = DDL.map(sql => dbHandle.prepare(sql));
  getDBResult('restoreTable', table.name, await dbHandle.batch(createBatch));

  // Now that the table is set up, we can actually pull in the data we are going
  // to insert.
  const data = JSON.parse(await dataStream.text());

  // Set up our eventual return.
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
  const insert = dbHandle.prepare(`
    INSERT INTO ${table.name}
             (${table.columns.join(', ')})
      VALUES (${Array(table.columns.length).fill('?').join(', ')})`
  );

  // Create a batch insert that will insert all of the data for the table into
  // the database.
  const insertBatch = data.map(e => insert.bind(...e));
  await dbHandle.batch(insertBatch);

  // Return the result object back.
  return result;
}


/******************************************************************************/


/* Perform a restore of a database backup using a collection of bare data files
 * sitting in the R2 bucket.
 *
 * fromDatabase and name are used to construct the top level R2 key under which
 * all of the data files will be contained. toDatabase is the name of the
 * database that is being restored into, and dbHandle is the actual bound D1
 * instance for that database.
 *
 * This will first look for and load the metadata.json file associated with the
 * backup, verify that none of the tables that it mentions exist already in the
 * database, and then in load order pull down and restore tables.
 *
 * The return value is the eventual success() or fail() of the request to do
 * the restore.
 *
 * On success, the return value provides information on the source and
 * destination database, the name of the database in question, and a list of all
 * of the restored tables along with a count of how many rows for each were
 * restored. */
async function performBareFileRestore(ctx, fromDatabase, toDatabase, dbHandle, name) {
  // All of our input files are going to come from R2 objects; this sets the
  // base key that is used for each such file.
  const baseKey = `${fromDatabase}/${name}`;

  // Try to fetch the metadata file; if this doesnt work, then we can't actually
  // restore anything because the main control file is missing.
  const metaKey = `${baseKey}/metadata.json`;
  const metadata = await r2FetchJSON(ctx, metaKey);
  if (metadata === null) {
    return fail(ctx, `metadata file '${metaKey}' not found`);
  }

  // Verify that none of the tables mentioned in the metadata exist; if they do,
  // then we're gonna be trying to insert data into the database when it is
  // already present.
  const existing = await verifyDbTables(ctx, dbHandle, metadata)
  if (existing.length !== 0) {
    return fail(ctx, `cannot restore; tables to be restored already exist`, 400, existing);
  }

  // Construct what will be our eventual return value.
  const result = {
    fromDatabase,
    toDatabase,
    name,
    baseKey,
    tables: []
  }

  // Going in table load order, grab the r2 object that contains the data for
  // that table, then restore it.
  //
  // If there is any error during this process, the database is left as-is to
  // allow for research to be done on what went wrong.
  for (const name of metadata.loadOrder) {
    // Get the table object and the underlying R2 object that holds the data
    const table = metadata.tables[name];
    const object = await r2RawGet(ctx, `${baseKey}/${name}.json`)
    if (object === null) {
      return fail(ctx, `cannot restore; table data file '${baseKey}/${name}.json' missing`);
    }

    // Restore the table to the database and push the result into the return
    // value.
    const contents = await restoreTable(ctx, dbHandle, table, object);
    result.tables.push(contents);
  }

  // Return the final result back
  return success(ctx, 'backup restored from file collection', result);
}


/******************************************************************************/


/* Perform a restore of a database backup using a tarball that is sitting in the
 * R2 bucket. The tarball can be either a bare tar or a compressed tgz file.
 *
 * fromDatabase and name are used to construct the R2 key that names the source
 * tarball to use. toDatabase is the name of the database that is being restored
 * into, and dbHandle is the actual bound D1 instance for that database.
 *
 * The tarball is required to have a specific order to the files contained
 * within it; the first file should be the metadata.json file, and each of the
 * remaining files should be in the archive in the same order as the metadata
 * loadOrder mentions.
 *
 * Checks are done to ensure that the files appear in the correct order and that
 * no files in the archive related to tables that are not known to be a part of
 * the backup.
 *
 * The return value is the eventual success() or fail() of the request to do
 * the restore.
 *
 * On success, the return value provides information on the source and
 * destination database, the name of the database in question, and a list of all
 * of the restored tables along with a count of how many rows for each were
 * restored. */
async function performTarRestore(ctx, fromDatabase, toDatabase, dbHandle, name) {
  // Get the name of the underlying R2 key that specifies the name of the
  // tarball to be used.
  const tarKey = `${fromDatabase}/${name}`;

  // Obtain the stream required for the provided key; bail if we can't find it.
  const tarball = await getTarStream(ctx, tarKey);
  if (tarball === null) {
    return fail(ctx, `file '${tarKey}' is not a valid tar file`);
  }

  // Small inline helper to ensure that a tarball member is a file and that it
  // has the name provided.
  const isFile = (m, name) => m.header.type === TAR_OBJECT_TYPE_FILE && m.header.name === name;

  // The list of files that we expect to be in the tarball, in archive order;
  // before we start this must be the metadata.json file. Once the metadata is
  // loaded, this will be expanded out.
  let fileList = ['metadata.json'];
  let metadata = null;

  // Construct what will be our eventual return value.
  const result = {
    fromDatabase,
    toDatabase,
    name,
    tarKey,
    tables: []
  }

  // Iterate over all of the members in the tarball and handle them.
  for await (const member of extract(tarball)) {
    // This member should be the first item in the fileList array and be a file;
    // if not, then this is not a valid backup tarball.
    if (isFile(member, fileList[0]) === false) {
      return fail(ctx, `unexpected file '${member.header.name}' in '${tarKey}'; expected '${fileList[0]}'`);
    }

    // Remove this file from the list of files we expect to see now that we've
    // seen it.
    fileList.splice(0, 1);

    // If we don't have any metadata yet, then we should load that and then
    // validate that everything is OK with it.
    if (metadata === null) {
      // The metadata is JSON; parse it, then add to the list of files that we
      // expect to see from the archive one file per table.
      metadata = JSON.parse(await member.text());
      fileList = metadata.loadOrder.map(e => `${e}.json`);

      // Verify that the database doesn't contain any of the tables that this
      // metadata is going to attempt to restore.
      const existing = await verifyDbTables(ctx, dbHandle, metadata)
      if (existing.length !== 0) {
        return fail(ctx, `cannot restore; tables to be restored already exist`, 400, existing);
      }

      // The metadata is handled now; skip to the next member.
      continue;
    }

    // This member represents a file that describes a specific table; pull the
    // extension off to get the table name, then look up the table in the
    // metadata to get the object that describes it.
    const tableName = member.header.name.split('.')[0];
    const table = metadata.tables[tableName];

    // If there is no such table, then we can't proceed with the restore.
    if (table === undefined) {
      return fail(ctx, `cannot restore; tarball contains entry for table ${tableName} but the metadata.json does not mention it`);
    }

    // Restore this table and add the result to the eventual return value.
    const contents = await restoreTable(ctx, dbHandle, table, member);
    result.tables.push(contents);
  }

  return success(ctx, 'backup restored from tarball', result);
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
  // Grab the database and backup names out of the body.
  const { fromDatabase, toDatabase, name } = ctx.req.valid('json');

  // Pull the destination database binding out of the context; if this does not
  // match a know DB, generate an error.
  const dbHandle = ctx.env[toDatabase];
  if (dbHandle === undefined) {
    return fail(ctx, `no such bound database '${toDatabase}'`);
  }

  // If the key prefix ends in .tar or .tgz, then this is a tar restore, so
  // stream the data from the tarball.
  if (name.endsWith('.tar') === true || name.endsWith('.tgz') === true) {
    return await performTarRestore(ctx, fromDatabase, toDatabase, dbHandle, name);
  }

  // The restore must be from a collection of files; do a bare file restore.
  return await performBareFileRestore(ctx, fromDatabase, toDatabase, dbHandle, name);
}


/******************************************************************************/
