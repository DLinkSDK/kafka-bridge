local _M = {}

--Polling pull time in seconds
_M.loop_time = 5

-- /kafka/EA3F80826F05C123B3CA9BD10B75A23D/get.txt Path for the default directory, modify need pairing with the server
--maxbuf=4000 indicates that a batch should be as large as 4000KB before being compressed and sent, and can also be replaced with maxline to indicate how many batches should be compressed and sent.
-- The form of the argument is not recommended to be changed
--partition indicates the number of partitions and is a constant
--
local kafka_uri_1 = "http://{kafka2http serer addr/domain}/kafka/EA3F80826F05C123B3CA9BD10B75A23D/get.txt?maxbuf=4000&partition="

-- loop_task: {R port, W port, P num, URL}
-- 例如 {8101, 8201, 20, kafka_uri_1},
-- 8101 Indicates which kafka2tcp program is consumed by the remote, and the consumed port corresponds to a different topic
-- 8201 The tcp2kafka program, which indicates which port was used locally, also corresponds to a different topic
-- 20   Number of partitions
-- kafka_uri_1 Indicates which connection to read, and the ser side

_M.loop_task = {
	{8101, 8201, 20, kafka_uri_1},
	{8121, 8221, 3, kafka_uri_1},
}

_M.token = "0123456789abcdef"

return _M

