Fixed crashes in the ``rocketmq_proxy`` network filter (contrib) when a request or response header
has an ``extFields`` object that is missing or lacks a required field, or a numeric field that is
not a valid integer. The codec now treats such frames as decode errors and closes the connection
instead of aborting the process.
