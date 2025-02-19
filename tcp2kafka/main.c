#include<time.h>
#include<stdio.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<string.h>
#include <stdlib.h>
#include <signal.h>
#include <ctype.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>
#include "tcp2kafka.h"
#include <pthread.h>

int port = 8200;
int run = 1;
int listenfd = 0;

extern void monitor();
extern void worker(void *arg);
extern pthread_mutex_t mutex;
extern void init_rk_pool();
extern void clean_conn();

char ini_file[512] = { 0 };
char topic_name[256] = { 0 };
struct ini_list kafka_list[20];

static void stop(int sig) {
	clean_conn();
	run = 0;
	close(listenfd);
	pthread_mutex_destroy(&mutex);
}

static void _trim(char *s)
{
	short   i, l, r, len;

	for(len=0; s[len]; len++);
	for(l=0; (s[l]==' ' || s[l]=='\t' || s[l]=='\n' || s[l]=='\r'); l++);
	if(l==len)
	{
		s[0]='\0';
		return;
	}
	for(r=len-1; (s[r]==' ' || s[r]=='\t' || s[r]=='\n' || s[r]=='\r' ); r--);
	for(i=l; i<=r; i++) s[i-l]=s[i];
	s[r-l+1]='\0';
	return;
}

void init_kafka()
{
	int i = 0;
	char buf[512] = { 0 };
	FILE *fp = fopen(ini_file, "r");
	if (!fp)
	{
		ACCESS_PRINT("fopen(kafka.ini) err.\n");
		run = 0;
		return;
	}

	while(fgets(buf, 511, fp))
	{

		_trim(buf);
		if (buf[0] == '#') continue;
		char *p = strchr(buf, '=');
		if (p)
		{
			*p = 0x00;
			_trim(buf);
			_trim(p+1);
			if (0 == strcmp(buf, "topic.name"))
			{
				strncpy(topic_name, p + 1, 255);
			}else if(0 == strcmp(buf, "port"))
			{
				port = atoi(p + 1);
			}
			else
			{
				strncpy(kafka_list[i].k, buf, 127);
				strncpy(kafka_list[i].v, p + 1, 255);
				i++;
			}
			if(i > 19)
				break;

			kafka_list[i].k[0] = 0x00;
		}
	}
	fclose(fp);
}

int main(int argc, char **argv)
{
	pthread_t monitor_thread, new_thread;
	struct sockaddr_in servaddr,cliaddr;
	socklen_t len = sizeof(struct sockaddr);
	int connfd, opt = 1;

	signal(SIGINT, stop);

	if (argc > 1)
		strncpy(ini_file, argv[1], 255);
	else
		strcpy(ini_file, "kafka.ini");

	init_kafka();

	listenfd = socket(AF_INET, SOCK_STREAM, 0);
	if(listenfd < 0)
	{
		ACCESS_PRINT("Socket created failed.\n");
		return -1;
	}

	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	ACCESS_PRINT("listen port:[%d]\n", port);

	servaddr.sin_family = AF_INET;
	servaddr.sin_port = htons(port);
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	if(bind(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0)
	{
		ACCESS_PRINT("Bind failed.\n");
		return -1;
	}

	ACCESS_PRINT("listening...\n");
	listen(listenfd, 500);

	init_rk_pool();

	pthread_mutex_init(&mutex, NULL);

	pthread_create(&monitor_thread, NULL, (void*)(&monitor), NULL);

	while(run)
	{
		if((connfd = accept(listenfd, (struct sockaddr*)&cliaddr, &len)) < 0 )
		{
			ACCESS_PRINT("accept failed.\n");
		}

		int *arg = (int *)malloc(sizeof(int));
		*arg = connfd;

		if (pthread_create(&new_thread, NULL, (void*)(&worker), (void*)arg) < 0) {
            perror("Could not create thread.\n");
            close(connfd);
            continue;
        }
		pthread_detach(new_thread);
	}
	return 0;
}

