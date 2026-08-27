-- Add the `server_client_expiration` column to `device`, matching the field
-- main added to `Device` (deadline the server pushed via
-- `<ib><client_expiration>`). JSON-encoded `ServerClientExpiration`, same as the
-- SQLite backend; nullable because the deadline is absent until the server
-- issues one.
ALTER TABLE device
    ADD COLUMN server_client_expiration TEXT;
