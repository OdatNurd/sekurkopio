# Sekurkopio - Simple Cloudflare Backup Tool

Sekurkopio (Esperanto for "Backup") is simple Cloudflare Worker Application for
backing up the contents of a Cloudflare D1 database to an R2 bucket and then
later restoring that backup into the same or a different database.

This particular project is intended to be entirely cloud based and is using the
following technology:

- [Cloudflare Zero Trust](https://www.cloudflare.com/zero-trust/) for IAM
- [Cloudflare Workers](https://www.cloudflare.com/developer-platform/workers/) to host the functions driving the API
- [Cloudflare D1](https://www.cloudflare.com/developer-platform/d1/) as the backing database
- [Cloudflare R2](https://www.cloudflare.com/developer-platform/r2/) for object storage
