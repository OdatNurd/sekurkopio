/******************************************************************************/


/* Check the R2 backup bucket to see if an object with the given key exists or
 * not; if it does, the object metadata is returned back; otherwise, null is
 * returned instead. */
export async function r2KeyExists(ctx, key) {
  console.log(`checking R2 for key: ${key}`);
  return await ctx.env.R2.head(key);
}


/******************************************************************************/


/* Given an R2 asset key and an object, encode the object to JSON and back it
 * up to the backup bucket under the given key.
 *
 * The object will be marked as being JSON data. */
export async function r2StoreJson(ctx, key, obj) {
  console.log(`storing JSON to R2 key: ${key}`);

  return ctx.env.R2.put(key, JSON.stringify(obj), {
    'httpMetadata': { 'contentType': 'application/json' },
  });
}


/******************************************************************************/


/* Given an R2 asset key, look for an object in the bucket that has that key,
 * grab it and convert it's content into a JSON object and return it.
 *
 * If there is no such key, null is returned instead. */
export async function r2FetchJSON(ctx, key) {
  console.log(`fetching JSON from R2 key: ${key}`);

  // Grab the object from the bucket and, if found, load it into JSON data and
  // return the resulting object back.
  const r2Object = await ctx.env.R2.get(key);
  if (r2Object !== null) {
    return await r2Object.json();
  }

  // Key was not found.
  console.log(`R2 key not found: ${key}`);
  return null;
}


/******************************************************************************/


/* Given an R2 asset key, look for an object in the bucket that has that key,
 * grab it and return it back. This will return the raw object; it is up to the
 * caller to do something with the body stream.
 *
 * If there is no such key, null is returned instead. */
export async function r2RawGet(ctx, key) {
  console.log(`fetching raw object from R2 key: ${key}`);

  // Grab the object from the bucket and, if found, load it into JSON data and
  // return the resulting object back.
  const r2Object = await ctx.env.R2.get(key);
  if (r2Object !== null) {
    return r2Object;
  }

  // Key was not found.
  console.log(`R2 key not found: ${key}`);
  return null;
}


/******************************************************************************/
