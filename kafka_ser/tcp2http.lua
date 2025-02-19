local ngx_socket_tcp = ngx.socket.tcp
local koala          = require "koala"
local zlib           = require "zlib"
local shm            = ngx.shared.cache_kafka
local cfg            = require "koala.kafka.config"

local args = ngx.req.get_uri_args()
if args.test then
	ngx.say(args.test, " ", ngx.time())
	ngx.exit(200)
end

if not args.maxline and not args.maxbuf then
	ngx.exit(404)
end

if not args.partition or not args.port or not args.ts or not args.ots then
	ngx.exit(404)
end

local tk = ngx.md5(table.concat({cfg.token, args.ts,args.ots,args.port,args.partition},""))
if tk ~= args.tk then
	ngx.exit(403)
end


local resty_lock = require "resty.lock"
local lock, err = resty_lock:new("kafka_lock",{timeout = 60,exptime = 60})
if not lock then
        ngx.log(ngx.ERR,"failed to create lock: ", err)
end
local elapsed, err = lock:lock("kafka_lock_key"..args.port..args.partition)
if nil ~= err then
	ngx.log(ngx.ERR,"lock: ", elapsed, ", ", err)
end

local key = "k" .. args.port .. args.partition
local ok, _ = shm:get(key)

if ok then
	local cache_ts = tonumber(ok)
	local ots = tonumber(args.ots)
	--ngx.log(ngx.ERR, "debug cache_ts:", cache_ts, " ots:", ots)
	if ots and ots ~= 0 then
		if cache_ts > ots then
			local val, _ = shm:get(key .. "buff")
			if val then
				if val ~= "00000000" then
					ngx.header["X-Content-Type"] = "true"
				end
				ngx.print(val)
				ngx.log(ngx.ERR, "respone cache.")
				local ok, err = lock:unlock()
				if not ok then
					ngx.log(ngx.ERR,"failed to unlock: ", err)
				end
				ngx.exit(200)
			end
		end
	end
end

local sock, err = ngx_socket_tcp()
if not sock then
	local ok, err = lock:unlock()
	if not ok then
		ngx.log(ngx.ERR,"failed to unlock: ", err)
	end
	ngx.exit(500)
end

sock:settimeout(60000)

local t1, t2, t3 = 0, 0, 0
local t0 = ngx.time()

local c, err = sock:connect("127.0.0.1", tonumber(args.port))
if not c then
	ngx.log(ngx.ERR, 'connect err: ', err)
	local ok, err = lock:unlock()
	if not ok then
		ngx.log(ngx.ERR,"failed to unlock: ", err)
	end
	ngx.exit(500)
end
t1 = ngx.time()
local sdata = table.concat({ args.maxline or 0, args.maxbuf or 0, args.partition, args.ts or '-' }, "|")
local send_len = string.format("%04d", #sdata)
--ngx.log(ngx.ERR, "send_len: ", send_len)

local bytes, err = sock:send({send_len, sdata})
if not bytes then
	ngx.log(ngx.ERR, 'send err: ', err)
	local ok, err = lock:unlock()
	if not ok then
		ngx.log(ngx.ERR,"failed to unlock: ", err)
	end
	ngx.exit(500)
end
t2 = ngx.time()
local ilen, data, data_len
local ret_table = {}
while true do
	ilen, err = sock:receive(8)
	if not ilen then
		ngx.log(ngx.ERR, 'recv err: ', err, " time debug: ", table.concat({t0, t1, t2, t3, ngx.time()}, " "))
		table.insert(ret_table, "00000000")
		break
	end

	--ngx.print(ilen)
	table.insert(ret_table, ilen)

	data_len = tonumber(ilen, 16)
	if data_len == 0 then
		break
	end

	t3 = ngx.time()
	data, err = sock:receive(data_len)
	if not ilen then
		ngx.log(ngx.ERR, 'recv err: ', err)
		ngx.exit(500)
	end
	--ngx.print(data)
	table.insert(ret_table, data)
end

if not err then
	sock:close()
else
	ngx.log(ngx.ERR,err)
end

if #ret_table == 1 then
	local ok, err = shm:set(key, args.ts)
	if err then
		ngx.log(ngx.ERR, "set key, err:", err)
	end
	ok, err = shm:set(key .. "buff", ret_table[1])
	if err then
		ngx.log(ngx.ERR, "set buff, err:", err)
	end
	local ok, err = lock:unlock()
	if not ok then
		ngx.log(ngx.ERR,"failed to unlock: ", err)
	end
	ngx.print(ret_table[1])
	ngx.exit(200)
end

local ret_data = zlib.deflate()(table.concat(ret_table), "finish")
if ret_data then
	local ok, err = shm:set(key, args.ts)
	if err then
		ngx.log(ngx.ERR, "set key, err:", err)
	end
	ok, err = shm:set(key .. "buff", ret_data)
	if err then
		ngx.log(ngx.ERR, "set buff, err:", err)
	end
	local ok, err = lock:unlock()
	if not ok then
		ngx.log(ngx.ERR,"failed to unlock: ", err)
	end
	ngx.header["X-Content-Type"] = "true"
	ngx.print(ret_data)
else
	ngx.exit(500)
end

