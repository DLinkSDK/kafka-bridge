void debug_log(char *fname, char *frm, ...);

#define ACCESS_PRINT(...) \
                debug_log("log/access-t2k.log", __VA_ARGS__)

#define ERR_PRINT(...) \
                debug_log("log/error-t2k.log", __VA_ARGS__)

struct ini_list
{
	char k[128];
	char v[256];
};
