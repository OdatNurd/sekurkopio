/******************************************************************************/


import { z } from 'zod';


/******************************************************************************/


/* During parsing of data, this can be used as part of a transform stage in a
 * schema to coerce the value into a number.
 *
 * The return value is a number, but errors can be flagged if the value is not
 * a valid number and the number is required.
 *
 * If the value does not convert into a number, and it's not required, then the
 * return value will be undefined as an indication of this. */
export function asNumber(isRequired) {
  // The underlying validation mechanism does not allow for extra arguments,
  // so return back a wrapped version of the actual function that will be used
  // so that it can close over our arguments here.
  return function(value, zCtx) {
    const parsed = Number(value);
    if (isNaN(parsed) === true) {
      // If the value is not strictly required, return undefined instead.
      if (isRequired === false) {
        return undefined;
      }

      // Flag this as an issue
      zCtx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Not a number",
      });

      return z.NEVER;
    }

    // All good
    return parsed;
  }
}


/******************************************************************************/


/* During parsing of data, this can be used as part of a transform stage in a
 * schema to coerce the value into a number if possible, falling back to a
 * string value if the value is not a number.
 *
 * The return value is always either a number or a string; number is only ever
 * returned for input fields that can be coerced directly to a number. */
export function numberOrString(value, zCtx) {
  const parsed = Number(value);
  if (isNaN(parsed) === true) {
    return value;
  }

  return parsed;
}


/******************************************************************************/