Fixed crashes and a busy loop in the ``rocketmq_proxy`` network filter (contrib) when a pop-message
response body from the broker is truncated or has invalid length fields. The filter now
bounds-checks the message fields before reading them and stops processing the body at the first
malformed message.
