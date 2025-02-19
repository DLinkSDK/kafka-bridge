#include <stdarg.h>
#include <stdio.h>

void debug_log(char *fname, char *frm, ...)
{
        FILE *fp;
        char buf[2048] = { 0 };
        va_list  ap;

        if (*frm)
        {
                va_start(ap,frm);
                vsnprintf(buf, 2000, frm, ap);
                va_end(ap);
        }

        if (( fp = fopen(fname, "a+")) == NULL)
                return;

        fprintf(fp, "%s\n", buf);

        fclose(fp);
}
