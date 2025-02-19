local ngx_socket_tcp    = ngx.socket.tcp
local http              = require "mylib.http"
local json              = require "cjson"
local zlib              = require "zlib"
local cfg               = require "koala.kafka.config"

local _M = {}

function _M.get_kafka_msg(url)
    local httpc = http.new()

    httpc:set_timeout(60 * 1000)

    local res, err = httpc:request_uri(url,
    {
        method = "GET",
        headers = {
            ["Connection"] = "keep-alive",
            ["User_agent"] = "kaola-kafka2http",
            ["Accept"] = "*/*",
        }
    })

    if not res then
        return nil, {}
    else
        if res.status == 200 then
			--ngx.log(ngx.ERR, "gzip flags: ", res.headers["X-Content-Type"])
			if res.headers["X-Content-Type"] then
				local stream = zlib.inflate()
            	return stream(res.body, "finish")
			else
            	return res.body
			end
        else
            return nil
        end
    end
end

function _M.create_sock(port)
	local sock, err = ngx_socket_tcp()
	if not sock then
		return nil
	end

	sock:settimeout(90 * 1000)

	local c, err = sock:connect("127.0.0.1", port)
	if not c then
		ngx.log(ngx.ERR, 'connect err: ', err)
		return nil
	end

	return sock
end

return _M

