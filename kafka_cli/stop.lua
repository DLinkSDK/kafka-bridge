local shm     = ngx.shared.kafka_ctl

local ok, err = shm:safe_set("ctl", "0")
if err then
	ngx.log(ngx.ERR, "write loop-ctl err: ", err)
	ngx.say(err)
else
	ngx.say("ok")
end

