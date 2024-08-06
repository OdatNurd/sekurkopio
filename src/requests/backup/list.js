/******************************************************************************/


import { success } from '#requests/common';

import { dbBkpGetList } from "#db/backup";


/******************************************************************************/


/* Handle a request to find the list of all available backups. */
export async function reqDumpList(ctx) {
  const backups = await dbBkpGetList(ctx.env.sekurkopio);
  return success(ctx, `found ${backups.length} backup(s)`, backups);
}


/******************************************************************************/
