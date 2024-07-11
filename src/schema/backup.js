/******************************************************************************/


import { z } from 'zod';

import strftime from 'strftime';


/******************************************************************************/


/* Generate a default value for a bucket key by using the current date and time,
 * including the seconds portion in order to avoid a clobber as much as is
 * possible. */
const generateKey = () => strftime("%Y%m%d_%H%M%S");


/******************************************************************************/


/* When generating a backup this specifies the data that should be provided in
 * the request.
 *
 * The backup is generated from a specific database to files in the R2 bucket
 * with a folder named for the source database and the provided name.
 *
 * The name is optional here; if not provided a date/time stamp is used. */
export const BackupCreateSchema = z.object({
  fromDatabase: z.string().regex(/^[\w-]+$/),
  name: z.string().regex(/^[\w-]+$/).default(generateKey),
});


/******************************************************************************/


/* When restoring a backup this specifies the data that should be provided in
 * the request.
 *
 * The backup is restored into the given destination database, but the original
 * source database and name used during the backup creation are required to know
 * where to find the files. */
export const BackupRestoreSchema = z.object({
  fromDatabase: z.string().regex(/^[\w-]+$/),
  toDatabase: z.string().regex(/^[\w-]+$/),
  name: z.string().regex(/^[\w-.]+$/),
});


/******************************************************************************/
