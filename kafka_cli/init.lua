local process = require "ngx.process"
local cfg     = require "koala.kafka.config"
local lib     = require "koala.kafka.common"
local shm     = ngx.shared.kafka_ctl

ngx.log(ngx.ERR, "process type:", process.type())

local function init(premature, p, task_num, ots)
	if premature then
		ngx.log(ngx.ERR, ngx.worker.pid(), " reload...")
		return
	end

	if p >= cfg.loop_task[task_num][3] then
		return
	end

	-- debug shm
	local flags = shm:get("ctl")

	if tonumber(flags) == 0 then
		ngx.log(ngx.ERR, "p:[", p, "] exit...")
		return
	end

	local t0 = ngx.now()*1000 -- ts + port + p = 唯一ID

	local sp = string.format("%d|%d", p, t0)
	local hlen = string.format("%04d", #sp)

	local t1, t2, t3 = 0, 0, 0
	local new_ots = 0

	local sock = lib.create_sock(cfg.loop_task[task_num][2])
	t1 = ngx.time()
	if sock then
		local tk = ngx.md5(table.concat({cfg.token, t0, ots,cfg.loop_task[task_num][1],p},""))
		local sdata = lib.get_kafka_msg(cfg.loop_task[task_num][4] .. p .. "&port=" .. cfg.loop_task[task_num][1] .. "&ts=" .. t0 .. "&ots=" .. ots.."&tk="..tk)
		local bytes, err

		if sdata then
			new_ots = t0
			t2 = ngx.time()
			bytes, err = sock:send({hlen, sp, sdata})
			if err then
				ngx.log(ngx.ERR, "send(data) err: ", err, " info: ", task_num, " ts: ", t0, " p: ", p)
				sock:close()
				goto END
			end
		else
			new_ots = ots
			ngx.log(ngx.ERR, "lib.get_kafka_msg err!", " info: ", task_num, " ts: ", t0, " p: ", p)
			sock:close()
			goto END
		end

		if not bytes then
			ngx.log(ngx.ERR, 'send err: ', err)
			sock:close()
			goto END
		end

		t3 = ngx.time()
		bytes, err = sock:receive(8)
		if not bytes then
			ngx.log(ngx.ERR, 'recv err: ', err, " time: ", table.concat({t0, t1, t2, t3, ngx.time()}, " "), " info: ", task_num, " p: ", p)
			sock:close()
			goto END
		else
			if 0 ~= tonumber(bytes) then
				ngx.log(ngx.ERR, "tcp2kafka return error!", " info: ", task_num, " ts: ", t0, " p: ", p, " err: ", err)
			end
		end
	else
		new_ots = ots
		ngx.log(ngx.ERR, "create_sock err!")
	end

	if sock then
		sock:close()
	end
::END::
	collectgarbage("collect")
	ngx.timer.at(cfg.loop_time, init, p, task_num, new_ots)
end

if "worker" == process.type() then
	local id = ngx.worker.id()
	local ok, err = shm:safe_set("ctl", "1")
	if err then
		ngx.log(ngx.ERR, "write loop-ctl err: ", err)
	end

	for i = 1 , #cfg.loop_task do
        local ok, err = ngx.timer.at(2 + (id % 5), init, tonumber(id), i, 0)
        if not ok then
            ngx.log(ngx.ERR, "failed to create init() timer: ", err)
        else
            ngx.log(ngx.DEBUG, "run ok!")
        end
	end
end

