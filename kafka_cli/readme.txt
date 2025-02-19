The client will need to install openresty

https://openresty.org/en/download.html



The default installation path is /usr/local/openresty



Creating a directory

mkdir -p /usr/local/openresty/lualib/koala/kafka



Place the lua file in the directory you created

mv *.lua /usr/local/openresty/lualib/koala/kafka



Replace the nginx.conf configuration file

mv nginx.conf /usr/local/openresty/nginx/conf/nginx.conf



Need to open the 8001 local port



Start the openresty program to work



kafka's configuration file is config.lua

Shut down the running method: http://127.0.0.1/stop

