/******************************************************************************/


import { Hono } from 'hono'

import { wrappedRequest as _, validate } from '#requests/common';

import { reqCreateDump } from '#requests/backup/create';
import { reqRestoreDump } from '#requests/backup/restore';
import { reqDumpList } from '#requests/backup/list';

import { BackupCreateSchema, BackupRestoreSchema } from '#schema/backup'

/******************************************************************************/


/* Create a small "sub-application" to wrap all of our routes, and then
 * map all routes in. */
export const backup = new Hono();


backup.get('/list',
        ctx => _(ctx, reqDumpList));


backup.get('/create',
        validate('json', BackupCreateSchema),
        ctx => _(ctx, reqCreateDump));


backup.put('/restore',
        validate('json', BackupRestoreSchema),
        ctx => _(ctx, reqRestoreDump));


/******************************************************************************/
