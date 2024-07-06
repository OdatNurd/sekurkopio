import { Hono } from 'hono';

import { server_info } from '#requests/server_info/index';
import { backup } from '#requests/backup/index';


/******************************************************************************/


/* The Hono application that we use for routing; by exporting this directly, it
 * will hook into the appropriate Cloudflare Worker infrastructure to allow us
 * to handle requests. */
const app = new Hono();

/* The current API version; this prefixes all of our routes. */
const APIV1 = '/api/v1'


/*******************************************************************************
 * Server API
 *******************************************************************************
 * The items in this section are related to getting information about the back
 * end server component that is running the application.
 ******************************************************************************/

app.route(`${APIV1}/server_info`, server_info);


/*******************************************************************************
 * Backup Routes
 *******************************************************************************
 * Items in this section have routes that mount on the root of the router and
 * are used to create and restore database backup dumps.
 ******************************************************************************/

app.route(`${APIV1}/backup`, backup);


/******************************************************************************/


export default app;
